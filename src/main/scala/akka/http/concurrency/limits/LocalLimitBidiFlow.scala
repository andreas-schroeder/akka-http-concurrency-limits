package akka.http.concurrency.limits

import akka.NotUsed
import akka.http.concurrency.limits.LimitBidiFolow._
import akka.http.concurrency.limits.LocalLimitBidiFlow._
import akka.stream.impl.Buffer
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage._
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import com.netflix.concurrency.limits.Limit

object LocalLimitBidiFlow {

  def apply[In, Out](limitAlgorithm: () => Limit,
                     rejection: In => Out,
                     result: Out => Outcome = (_: Out) => Processed,
                     parallelism: Int): BidiFlow[In, In, Out, Out, NotUsed] =
    BidiFlow.fromGraph(new LocalLimitBidi(limitAlgorithm, rejection, result, parallelism))

  class InFlightElement(val startTime: Long)

  type LimitShape[In, Out] = BidiShape[In, In, Out, Out]

  case class UnexpectedOutputException(element: Any)
      extends RuntimeException(
        s"Inner flow produced unexpected result element '$element' when no element was outstanding"
      )
  case class OutputTruncationException(missingOutputElements: Int)
      extends RuntimeException(
        s"Inner flow was completed without producing result elements for $missingOutputElements outstanding elements"
      )
}

class LocalLimitBidi[In, Out](limitAlgorithm: () => Limit,
                              rejection: In => Out,
                              result: Out => Outcome,
                              parallelism: Int,
                              clock: () => Long = () => System.nanoTime())
    extends GraphStage[LimitShape[In, Out]] {

  private val in = Inlet[In]("LimitBidiFlow.in")
  private val toWrapped = Outlet[In]("LimitBidiFlow.toWrapped")
  private val fromWrapped = Inlet[Out]("LimitBidiFlow.fromWrapped")
  private val out = Outlet[Out]("LimitBidiFlow.out")

  def shape: LimitShape[In, Out] = BidiShape(in, toWrapped, fromWrapped, out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val limitInstance: Limit = limitAlgorithm()

      var limit: Int = limitInstance.getLimit
      limitInstance.notifyOnChange(l => limit = l)

      var inFlightElements: Buffer[InFlightElement] = _
      var pullSuppressed = false

      override def preStart(): Unit = {
        inFlightElements = Buffer(parallelism, inheritedAttributes)
      }

      setHandler(
        in,
        new InHandler {
          def onPush(): Unit = {
            val elem = grab(in)
            if (inFlightElements.used >= limit) {
              push(out, rejection(elem))
            } else {
              inFlightElements.enqueue(new InFlightElement(clock()))
              push(toWrapped, elem)
            }
          }
          override def onUpstreamFinish(): Unit = complete(toWrapped)
        }
      )

      setHandler(toWrapped, new OutHandler {
        override def onPull(): Unit =
          if (inFlightElements.used < inFlightElements.capacity) pull(in) else pullSuppressed = true

        override def onDownstreamFinish(cause: Throwable): Unit = cancel(in, cause)
      })

      setHandler(
        fromWrapped,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(fromWrapped)
            if (inFlightElements.isEmpty) {
              failStage(UnexpectedOutputException(elem))
            } else {
              val accepted = inFlightElements.dequeue()
              result(elem) match {
                case Processed =>
                  val duration = clock() - accepted.startTime
                  limitInstance.onSample(accepted.startTime, duration, inFlightElements.used, false)
                case Dropped =>
                  val duration = clock() - accepted.startTime
                  limitInstance.onSample(accepted.startTime, duration, inFlightElements.used, true)
                case Ignored => ()
              }
              push(out, elem)
              if (pullSuppressed) {
                tryPull(in)
                pullSuppressed = false
              }
            }
          }

          override def onUpstreamFinish(): Unit = {
            if (inFlightElements.used > 0) failStage(OutputTruncationException(inFlightElements.used))
            else completeStage()
          }
        }
      )

      setHandler(out, new OutHandler {
        // when the last element was rejected, 'fromWrapped' is already pulled and
        // 'in' isn't pulled. So we need to pull the right inlet.
        override def onPull(): Unit = if (hasBeenPulled(fromWrapped)) pull(in) else pull(fromWrapped)
        override def onDownstreamFinish(cause: Throwable): Unit = cancel(fromWrapped, cause)
      })
    }
}
