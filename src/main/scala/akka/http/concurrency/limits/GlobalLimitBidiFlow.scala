package akka.http.concurrency.limits

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.http.concurrency.limits.LimitActor._
import akka.http.concurrency.limits.GlobalLimitBidiFlow.LimitShape
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.impl.Buffer
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage._
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object GlobalLimitBidiFlow {
  def apply(
    limiter: ActorRef[RequestReceived]
  ): BidiFlow[HttpRequest, HttpRequest, HttpResponse, HttpResponse, NotUsed] =
    BidiFlow.fromGraph(new GlobalLimitBidi(limiter))

  type LimitShape = BidiShape[HttpRequest, HttpRequest, HttpResponse, HttpResponse]
}

class GlobalLimitBidi(limiter: ActorRef[RequestReceived]) extends GraphStage[LimitShape] {

  private final val tooManyRequestsResponse =
    HttpResponse(StatusCodes.TooManyRequests, entity = "Too many requests")

  private val in = Inlet[HttpRequest]("LimitBidiFlow.in")
  private val toWrapped = Outlet[HttpRequest]("LimitBidiFlow.toWrapped")
  private val fromWrapped = Inlet[HttpResponse]("LimitBidiFlow.fromWrapped")
  private val out = Outlet[HttpResponse]("LimitBidiFlow.out")

  def shape: LimitShape =
    BidiShape(in, toWrapped, fromWrapped, out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      var inFlightAccepted: Buffer[RequestAccepted] = _
      var pullSuppressed = false

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
        inFlightAccepted = Buffer(parallelism, inheritedAttributes)
      }

      setHandler(in, new InHandler {
        def onPush(): Unit = {
          val request = grab(in)
          limiter.ask[LimitActorResponse](sender => RequestReceived(sender, request)).onComplete(limiterResponse.invoke)
        }
        override def onUpstreamFinish(): Unit = complete(toWrapped)
      })

      private val limiterResponse = createAsyncCallback[Try[LimitActorResponse]] {
        case Success(accept: RequestAccepted) =>
          inFlightAccepted.enqueue(accept)
          push(toWrapped, accept.request)

        case Success(RequestRejected) => push(out, tooManyRequestsResponse)
        case Failure(ex)              => failStage(ex)
      }

      setHandler(toWrapped, new OutHandler {
        override def onPull(): Unit =
          if (inFlightAccepted.used < inFlightAccepted.capacity) pull(in) else pullSuppressed = true

        override def onDownstreamFinish(cause: Throwable): Unit = cancel(in, cause)
      })

      setHandler(fromWrapped, new InHandler {
        override def onPush(): Unit = {
          val response = grab(fromWrapped)
          // ignores the time it takes to stream the response body to clients on purpose to avoid
          // slow clients or network to be considered in server response latency measurements.
          val accepted = inFlightAccepted.dequeue()
          response.status match {
            case _: StatusCodes.ServerError => accepted.ignore()
            case _: StatusCodes.ClientError => accepted.ignore()
            case _                          => accepted.success()
          }
          push(out, response)
          if (pullSuppressed) {
            tryPull(in)
            pullSuppressed = false
          }
        }
      })

      // when the last response was 'Too many requests', fromWrapped is already pulled and
      // in isn't pulled. So we need to pull the right inlet.
      setHandler(out, new OutHandler {
        // when the last element was rejected, 'fromWrapped' is already pulled and
        // 'in' isn't pulled. So we need to pull the right inlet.
        override def onPull(): Unit = if (hasBeenPulled(fromWrapped)) pull(in) else pull(fromWrapped)
        override def onDownstreamFinish(cause: Throwable): Unit = cancel(fromWrapped, cause)
      })

      // Akka Http tears down the request handler flow on request timeout, and therefore
      // any in-flight request at this time was dropped.
      override def postStop(): Unit = while (inFlightAccepted.nonEmpty) {
        inFlightAccepted.dequeue().dropped()
      }
    }
}
