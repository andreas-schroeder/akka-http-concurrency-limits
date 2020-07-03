package akka.http.concurrency.limits

import java.util.concurrent.TimeUnit

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl._
import akka.http.concurrency.limits.LimitActor._
import akka.http.scaladsl.model.HttpRequest
import com.netflix.concurrency.limits.Limit

import scala.collection.mutable
import scala.concurrent.duration._

class LiFoQueuedLimitActor(limitAlgorithm: Limit,
                           maxLiFoQueueDepth: Int,
                           maxDelay: HttpRequest => FiniteDuration,
                           context: ActorContext[LimitActorCommand],
                           timers: TimerScheduler[LimitActorCommand],
                           clock: () => Long = () => System.nanoTime())
    extends AbstractBehavior[LimitActorCommand](context) {

  private val inFlight: mutable.Set[RequestId] = mutable.Set.empty
  private val throttledLiFoQueue: mutable.Stack[RequestReceived] = mutable.Stack.empty
  private var limit: Int = limitAlgorithm.getLimit
  limitAlgorithm.notifyOnChange(l => limit = l)

  val serverRequestTimeout: FiniteDuration = {
    val timeout = context.system.settings.config.getDuration("akka.http.server.request-timeout")
    FiniteDuration(timeout.toNanos, TimeUnit.NANOSECONDS)
  }

  def onMessage(command: LimitActorCommand): Behavior[LimitActorCommand] = command match {
    case received: RequestReceived =>
      if (inFlight.size < limit) acceptRequest(received) else throttleOrDelay(received)
      this

    case RequestTimedOut(startTime, id) =>
      if (inFlight.remove(id)) {
        // raciness: timeout vs regular response are intentionally concurrent.
        limitAlgorithm.onSample(startTime, clock() - startTime, inFlight.size, true)
        maybeAcceptNext()
      }
      this

    case MaxRequestDelayPassed(sender, id) =>
      sender ! RequestRejected
      throttledLiFoQueue.filterInPlace(_.id eq id)
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

  private def throttleOrDelay(received: RequestReceived): Unit = {
    val delay = maxDelay(received.request)
    if (delay.length == 0 || throttledLiFoQueue.size >= maxLiFoQueueDepth * limit) {
      received.sender ! RequestRejected
    } else {
      throttledLiFoQueue.push(received)
      timers.startSingleTimer(received.id, MaxRequestDelayPassed(received.sender, received.id), delay)
    }
  }

  def maybeAcceptNext(): Unit = if (inFlight.size < limit && throttledLiFoQueue.nonEmpty) {
    val request = throttledLiFoQueue.pop()
    timers.cancel(request.id)
    acceptRequest(request)
  }

  private def acceptRequest(received: RequestReceived): Unit = {
    val id = received.id
    inFlight += id
    val accept = new RequestAccepted(received.request, context.self, clock, id)
    received.sender ! accept
    timers.startSingleTimer(id, RequestTimedOut(accept.startTime, id), serverRequestTimeout)
  }
}
