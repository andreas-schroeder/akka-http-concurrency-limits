package akka.http.concurrency.limits

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.http.concurrency.limits.LimitActor._
import akka.http.concurrency.limits.LimitBidiFlow.LimitBidiFlowShape
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.impl.Buffer
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage._
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object LimitBidiFlow {
  def apply(
    limiter: ActorRef[RequestReceived]
  ): BidiFlow[HttpRequest, HttpRequest, HttpResponse, HttpResponse, NotUsed] =
    BidiFlow.fromGraph(new LimitBidiFlow(limiter))

  type LimitBidiFlowShape =
    BidiShape[HttpRequest, HttpRequest, HttpResponse, HttpResponse]
}

class LimitBidiFlow(limiter: ActorRef[RequestReceived]) extends GraphStage[LimitBidiFlowShape] {

  private final val tooManyRequestsResponse =
    HttpResponse(StatusCodes.TooManyRequests, entity = "Too many requests")

  private val requestIn = Inlet[HttpRequest]("LimitBidiFlow.requestIn")
  private val requestOut = Outlet[HttpRequest]("LimitBidiFlow.requestOut")
  private val responseIn = Inlet[HttpResponse]("LimitBidiFlow.responseIn")
  private val responseOut = Outlet[HttpResponse]("LimitBidiFlow.responseOut")

  def shape: LimitBidiFlowShape =
    BidiShape(requestIn, requestOut, responseIn, responseOut)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      var inFlightAccepted: Buffer[RequestAccepted] = _

      implicit var serverRequestTimeout: Timeout = _
      implicit var scheduler: Scheduler = _
      implicit var ec: ExecutionContext = _

      override def preStart(): Unit = {
        val system = materializer.system
        scheduler = system.toTyped.scheduler
        ec = system.dispatcher

        val config = system.settings.config
        val timeout = config.getDuration("akka.http.server.request-timeout")
        serverRequestTimeout = Timeout(timeout.toNanos, TimeUnit.NANOSECONDS)

        val parallelism = config.getInt("akka.http.server.pipelining-limit")
        inFlightAccepted = Buffer(parallelism, parallelism + 1)
      }

      setHandler(requestIn, () => {
        val request = grab(requestIn)
        limiter.ask[LimitActorResponse](sender => RequestReceived(sender, request)).onComplete(limiterResponse.invoke)
      })

      private val limiterResponse = createAsyncCallback[Try[LimitActorResponse]] {
        case Success(accept: RequestAccepted) =>
          inFlightAccepted.enqueue(accept)
          push(requestOut, accept.request)

        case Success(RequestRejected) => push(responseOut, tooManyRequestsResponse)
        case Failure(ex)              => failStage(ex)
      }

      setHandler(requestOut, () => pull(requestIn))

      setHandler(responseIn, () => {
        val response = grab(responseIn)
        // ignores the time it takes to stream the response body to clients on purpose to avoid
        // slow clients or network to be considered in server response latency measurements.
        val accepted = inFlightAccepted.dequeue()
        response.status match {
          case _: StatusCodes.ServerError => accepted.ignore()
          case _: StatusCodes.ClientError => accepted.ignore()
          case _                          => accepted.success()
        }
        push(responseOut, response)
      })

      // when the last response was 'Too many requests', responseIn is already pulled and
      // requestIn isn't pulled. So we need to pull the right inlet.
      setHandler(responseOut, () => if (hasBeenPulled(responseIn)) pull(requestIn) else pull(responseIn))

      // Akka Http tears down the request handler flow on request timeout, and therefore
      // any in-flight request at this time was dropped.
      override def postStop(): Unit = while (inFlightAccepted.nonEmpty) {
        inFlightAccepted.dequeue().dropped()
      }
    }
}
