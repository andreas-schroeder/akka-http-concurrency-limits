package akka.http.concurrency.limits

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl._
import akka.http.concurrency.limits.LimitActor._
import com.netflix.concurrency.limits.Limit

import scala.collection.mutable
import scala.concurrent.duration._

class LiFoQueuedLimitActor(limitAlgorithm: Limit,
                              maxLiFoQueueDepth: Int,
                              maxDelay: FiniteDuration,
                              timeout: FiniteDuration,
                              context: ActorContext[LimitActorCommand],
                              timers: TimerScheduler[LimitActorCommand],
                              clock: () => Long = () => System.nanoTime())
    extends AbstractBehavior[LimitActorCommand](context) {

  private val inFlight: mutable.Set[Id] = mutable.Set.empty
  private val throttledLiFoQueue: mutable.Stack[Element] = mutable.Stack.empty
  private var limit: Int = limitAlgorithm.getLimit
  limitAlgorithm.notifyOnChange(l => limit = l)

  def onMessage(command: LimitActorCommand): Behavior[LimitActorCommand] = command match {
    case received: Element =>
      if (inFlight.size < limit) acceptElement(received) else rejectOrDelay(received)
      this

    case ElementTimedOut(element, startTime) =>
      if (inFlight.remove(element.id)) {
        // raciness: timeout vs regular response are intentionally concurrent.
        limitAlgorithm.onSample(startTime, clock() - startTime, inFlight.size, true)
        maybeAcceptNext()
      }
      this

    case MaxDelayPassed(elem) =>
      elem.sender ! ElementRejected
      throttledLiFoQueue.filterInPlace(_.id eq elem.id)
      this

    case Replied(start, duration, didDrop, weight, id) =>
      if (inFlight.remove(id)) {
        // raciness: timeout vs regular response are intentionally concurrent.
        timers.cancel(id)
        recordSamples(start, duration, didDrop, weight)
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

  private def recordSamples(start: Long, duration: Long, didDrop: Boolean, weight: Int): Unit = {
    if (weight == 1) {
      limitAlgorithm.onSample(start, duration, inFlight.size, didDrop)
    } else {
      val itemDuration = duration / weight;
      var i = 0
      var startTime = start
      while (i < weight) {
        limitAlgorithm.onSample(startTime, itemDuration, inFlight.size, didDrop)
        startTime += itemDuration
        i += 1
      }
    }
  }

  private def rejectOrDelay(element: Element): Unit = {
    if (maxDelay.length == 0 || throttledLiFoQueue.size >= maxLiFoQueueDepth * limit) {
      element.sender ! ElementRejected
    } else {
      throttledLiFoQueue.push(element)
      timers.startSingleTimer(element.id, MaxDelayPassed(element), maxDelay)
    }
  }

  def maybeAcceptNext(): Unit = if (inFlight.size < limit && throttledLiFoQueue.nonEmpty) {
    val element = throttledLiFoQueue.pop()
    timers.cancel(element.id)
    acceptElement(element)
  }

  private def acceptElement(element: Element): Unit = {
    inFlight += element.id
    element.sender ! new ElementAccepted(context.self, element.id)
    timers.startSingleTimer(element.id, ElementTimedOut(element, clock()), timeout)
  }
}
