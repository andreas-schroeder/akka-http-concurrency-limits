package akka.http.concurrency.limits

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl._
import akka.http.concurrency.limits.LimitActor._
import com.netflix.concurrency.limits.Limit

import scala.collection.mutable
import scala.concurrent.duration._

class LiFoQueuedLimitActor[T](limitAlgorithm: Limit,
                              maxLiFoQueueDepth: Int,
                              maxDelay: T => FiniteDuration,
                              timeout: FiniteDuration,
                              context: ActorContext[LimitActorCommand[T]],
                              timers: TimerScheduler[LimitActorCommand[T]],
                              clock: () => Long = () => System.nanoTime())
    extends AbstractBehavior[LimitActorCommand[T]](context) {

  private val inFlight: mutable.Set[Element[T]] = mutable.Set.empty
  private val throttledLiFoQueue: mutable.Stack[Element[T]] = mutable.Stack.empty
  private var limit: Int = limitAlgorithm.getLimit
  limitAlgorithm.notifyOnChange(l => limit = l)

  def onMessage(command: LimitActorCommand[T]): Behavior[LimitActorCommand[T]] = command match {
    case received: Element[T] =>
      if (inFlight.size < limit) acceptElement(received) else rejectOrDelay(received)
      this

    case ElementTimedOut(element, startTime) =>
      if (inFlight.remove(element)) {
        // raciness: timeout vs regular response are intentionally concurrent.
        limitAlgorithm.onSample(startTime, clock() - startTime, inFlight.size, true)
        maybeAcceptNext()
      }
      this

    case MaxDelayPassed(elem) =>
      elem.sender ! ElementRejected(elem.value)
      throttledLiFoQueue.filterInPlace(_ eq elem)
      this

    case Replied(start, duration, didDrop, id) =>
      if (inFlight.remove(id)) {
        // raciness: timeout vs regular response are intentionally concurrent.
        timers.cancel(id)
        limitAlgorithm.onSample(start, duration, inFlight.size, didDrop)
        maybeAcceptNext()
      }
      this

    case Ignore(id) =>
      // raciness: timeout vs regular response are intentionally concurrent.
      if (inFlight.remove(id)) {
        timers.cancel(id)
        maybeAcceptNext()
      }
      this
  }

  private def rejectOrDelay(element: Element[T]): Unit = {
    val delay = maxDelay(element.value)
    if (delay.length == 0 || throttledLiFoQueue.size >= maxLiFoQueueDepth * limit) {
      element.sender ! ElementRejected(element.value)
    } else {
      throttledLiFoQueue.push(element)
      timers.startSingleTimer(element, MaxDelayPassed(element), delay)
    }
  }

  def maybeAcceptNext(): Unit = if (inFlight.size < limit && throttledLiFoQueue.nonEmpty) {
    val element = throttledLiFoQueue.pop()
    timers.cancel(element)
    acceptElement(element)
  }

  private def acceptElement(element: Element[T]): Unit = {
    inFlight += element
    element.sender ! new ElementAccepted(context.self, element)
    timers.startSingleTimer(element, ElementTimedOut(element, clock()), timeout)
  }
}
