package akka.http.concurrency.limits

import java.util.concurrent.TimeUnit

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl._
import akka.http.concurrency.limits.LimitActor._
import com.netflix.concurrency.limits.Limit

import scala.collection.mutable
import scala.concurrent.duration._

class LiFoQueuedLimitActor[T](limitAlgorithm: Limit,
                           maxLiFoQueueDepth: Int,
                           maxDelay: T => FiniteDuration,
                           context: ActorContext[LimitActorCommand[T]],
                           timers: TimerScheduler[LimitActorCommand[T]],
                           clock: () => Long = () => System.nanoTime())
    extends AbstractBehavior[LimitActorCommand[T]](context) {

  private val inFlight: mutable.Set[Element[T]] = mutable.Set.empty
  private val throttledLiFoQueue: mutable.Stack[Element[T]] = mutable.Stack.empty
  private var limit: Int = limitAlgorithm.getLimit
  limitAlgorithm.notifyOnChange(l => limit = l)

  val serverRequestTimeout: FiniteDuration = {
    val timeout = context.system.settings.config.getDuration("akka.http.server.request-timeout")
    FiniteDuration(timeout.toNanos, TimeUnit.NANOSECONDS)
  }

  def onMessage(command: LimitActorCommand[T]): Behavior[LimitActorCommand[T]] = command match {
    case received: Element[T] =>
      if (inFlight.size < limit) acceptRequest(received) else throttleOrDelay(received)
      this

    case ElementTimedOut(element, startTime) =>
      if (inFlight.remove(element)) {
        // raciness: timeout vs regular response are intentionally concurrent.
        limitAlgorithm.onSample(startTime, clock() - startTime, inFlight.size, true)
        maybeAcceptNext()
      }
      this

    case MaxDelayPassed(sender, received) =>
      sender ! ElementRejected(received.value)
      throttledLiFoQueue.filterInPlace(_ eq received)
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

  private def throttleOrDelay(received: Element[T]): Unit = {
    val delay = maxDelay(received.value)
    if (delay.length == 0 || throttledLiFoQueue.size >= maxLiFoQueueDepth * limit) {
      received.sender ! ElementRejected(received.value)
    } else {
      throttledLiFoQueue.push(received)
      timers.startSingleTimer(received, MaxDelayPassed(received.sender, received), delay)
    }
  }

  def maybeAcceptNext(): Unit = if (inFlight.size < limit && throttledLiFoQueue.nonEmpty) {
    val request = throttledLiFoQueue.pop()
    timers.cancel(request)
    acceptRequest(request)
  }

  private def acceptRequest(received: Element[T]): Unit = {
    inFlight += received
    received.sender ! new ElementAccepted(context.self, received)
    timers.startSingleTimer(received, ElementTimedOut(received, clock()), serverRequestTimeout)
  }
}
