package akka.http.concurrency.limits

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.http.concurrency.limits.GlobalLimitBidiFlow.{InFlight, LimitShape}
import akka.http.concurrency.limits.LimitActor._
import akka.http.concurrency.limits.LimitBidiFolow.{Dropped, Ignored, Outcome, Processed}
import akka.stream.impl.Buffer
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage._
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

object GlobalLimitBidiFlow {
  def apply[In, Out](limiter: ActorRef[Element[In]],
                     parallelism: Int,
                     timeout: FiniteDuration,
                     rejection: In => Out,
                     result: Out => Outcome = (_: Out) => Processed): BidiFlow[In, In, Out, Out, NotUsed] =
    BidiFlow.fromGraph(new GlobalLimitBidi(limiter, rejection, result, parallelism, timeout))

  type LimitShape[In, Out] = BidiShape[In, In, Out, Out]

  final class InFlight[T](val accepted: ElementAccepted[T], val startTime: Long)
}

class GlobalLimitBidi[In, Out](limiter: ActorRef[Element[In]],
                               rejection: In => Out,
                               result: Out => Outcome,
                               parallelism: Int,
                               timeout: FiniteDuration,
                               clock: () => Long = () => System.nanoTime())
    extends GraphStage[LimitShape[In, Out]] {

  private val in = Inlet[In]("LimitBidiFlow.in")
  private val toWrapped = Outlet[In]("LimitBidiFlow.toWrapped")
  private val fromWrapped = Inlet[Out]("LimitBidiFlow.fromWrapped")
  private val out = Outlet[Out]("LimitBidiFlow.out")

  def shape: LimitShape[In, Out] =
    BidiShape(in, toWrapped, fromWrapped, out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    var inFlightAccepted: Buffer[InFlight[In]] = _
    var pullSuppressed = false

    implicit val askTimeout: Timeout = Timeout(timeout)
    implicit var scheduler: Scheduler = _
    implicit var ec: ExecutionContext = _

    override def preStart(): Unit = {
      val system = materializer.system
      scheduler = system.toTyped.scheduler
      ec = system.dispatcher
      inFlightAccepted = Buffer(parallelism, inheritedAttributes)
    }

    setHandler(
      in,
      new InHandler {
        def onPush(): Unit = {
          val value = grab(in)
          limiter
            .ask[LimitActorResponse[In]](sender => Element(sender, value))
            .onComplete(limiterResponse.invoke)
        }
        override def onUpstreamFinish(): Unit = complete(toWrapped)
      }
    )

    private val limiterResponse = createAsyncCallback[Try[LimitActorResponse[In]]] {
      case Success(accept: ElementAccepted[In]) =>
        inFlightAccepted.enqueue(new InFlight[In](accept, clock()))
        push(toWrapped, accept.value)

      case Success(ElementRejected(element)) => push(out, rejection(element))
      case Failure(ex)                       => failStage(ex)
    }

    setHandler(toWrapped, new OutHandler {
      override def onPull(): Unit =
        if (inFlightAccepted.used < inFlightAccepted.capacity) pull(in) else pullSuppressed = true

      override def onDownstreamFinish(cause: Throwable): Unit = cancel(in, cause)
    })

    setHandler(
      fromWrapped,
      new InHandler {
        override def onPush(): Unit = {
          val response = grab(fromWrapped)
          // ignores the time it takes to stream the response body to clients on purpose to avoid
          // slow clients or network to be considered in server response latency measurements.
          val inflight = inFlightAccepted.dequeue()
          import inflight._
          result(response) match {
            case Processed => accepted.success(startTime, clock())
            case Dropped   => accepted.dropped(startTime, clock())
            case Ignored   => accepted.ignore()
          }
          push(out, response)
          if (pullSuppressed) {
            tryPull(in)
            pullSuppressed = false
          }
        }
      }
    )

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
      val inFlight = inFlightAccepted.dequeue()
      inFlight.accepted.dropped(inFlight.startTime, clock())
    }
  }
}
