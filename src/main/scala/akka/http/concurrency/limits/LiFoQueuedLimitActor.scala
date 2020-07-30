package akka.http.concurrency.limits

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl._
import akka.http.concurrency.limits.LimitActor._
import com.netflix.concurrency.limits.Limit

import scala.collection.mutable
import scala.concurrent.duration._

class LiFoQueuedLimitActor(limitAlgorithm: Limit,
                           maxLiFoQueueDepth: Int,
                           batchSize: Int,
                           batchTimeout: FiniteDuration,
                           maxDelay: FiniteDuration,
                           timeout: FiniteDuration,
                           context: ActorContext[LimitActorMessage],
                           timers: TimerScheduler[LimitActorMessage],
                           clock: () => Long = () => System.nanoTime())
    extends AbstractBehavior[LimitActorMessage](context) {

  private val totalTimeout = timeout + batchTimeout
  private val batchTimeoutNanos = batchTimeout.toNanos
  private val inFlight: mutable.Set[Id] = mutable.Set.empty
  private val throttledLiFoQueue: mutable.Stack[RequestCapacity] = mutable.Stack.empty
  private var limit: Int = limitAlgorithm.getLimit
  limitAlgorithm.notifyOnChange(l => limit = l)

  def grant(id: Id): CapacityGranted = CapacityGranted(batchSize, clock() + batchTimeoutNanos, id)
  def reject: CapacityRejected = CapacityRejected(batchSize, clock() + batchTimeoutNanos)

  def onMessage(command: LimitActorMessage): Behavior[LimitActorMessage] = command match {
    case request: RequestCapacity =>
      if (inFlight.size < limit) acceptRequest(request) else rejectOrDelay(request)
      this

    case ReleaseCapacityGrant(id) =>
      if (inFlight.remove(id)) {
        // raciness: timeout vs regular release are intentionally concurrent.
        timers.cancel(id)
        maybeAcceptNext()
      }
      this

    case CapacityGrantTimedOut(id) =>
      if (inFlight.remove(id)) {
        // raciness: timeout vs regular release are intentionally concurrent.
        recordRequestDrop()
        maybeAcceptNext()
      }
      this

    case MaxDelayPassed(request) =>
      request.sender ! reject
      throttledLiFoQueue.filterInPlace(_.id eq request.id)
      this

    case replied: Replied =>
      recordSamples(replied)
      this

    case Ignore =>
      this
  }

  private def recordRequestDrop(): Unit = {
    val totalTimeoutNanos = totalTimeout.toNanos
    val now = clock()
    val startTime = now - totalTimeoutNanos
    limitAlgorithm.onSample(startTime, totalTimeoutNanos, inFlight.size, true)
  }

  private def recordSamples(replied: Replied): Unit = {
    import replied.{startTime, duration, weight, didDrop}
    if (weight == 1) {
      limitAlgorithm.onSample(startTime, duration, inFlight.size, didDrop)
    } else {
      val itemDuration = duration / weight
      var i = 0
      var start = replied.startTime
      while (i < weight) {
        limitAlgorithm.onSample(start, itemDuration, inFlight.size, didDrop)
        start += itemDuration
        i += 1
      }
    }
  }

  private def rejectOrDelay(request: RequestCapacity): Unit = {
    if (maxDelay.length == 0 || throttledLiFoQueue.size >= maxLiFoQueueDepth * limit) {
      request.sender ! reject
    } else {
      throttledLiFoQueue.push(request)
      timers.startSingleTimer(request.id, MaxDelayPassed(request), maxDelay)
    }
  }

  def maybeAcceptNext(): Unit = if (throttledLiFoQueue.nonEmpty && inFlight.size < limit) {
    val request = throttledLiFoQueue.pop()
    timers.cancel(request.id)
    acceptRequest(request)
  }

  private def acceptRequest(request: RequestCapacity): Unit = {
    request.sender ! grant(request.id)
    inFlight.addOne(request.id)
    timers.startSingleTimer(request.id, CapacityGrantTimedOut(request.id), totalTimeout)
  }
}
