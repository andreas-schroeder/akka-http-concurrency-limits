package akka.http.concurrency.limits

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model.HttpRequest
import com.netflix.concurrency.limits.Limit

import scala.concurrent.duration._

object LimitActor {

  /**
   *
   * @param limitAlgorithm adaptive concurrency limit algorithm to use.
   * @param maxLiFoQueueDepth max depth of the LiFo queue - gets multiplied with current limit to compute max queue length.
   * @param maxDelay compute max acceptable delay for requests to queue for being accepted.
   */
  def liFoQueued(limitAlgorithm: Limit,
            maxLiFoQueueDepth: Int,
            maxDelay: HttpRequest => FiniteDuration = _ => 0.nanos): Behavior[RequestReceived] =
    Behaviors
      .setup[LimitActorCommand](
        ctx =>
          Behaviors
            .withTimers(timers => new LiFoQueuedLimitActor(limitAlgorithm, maxLiFoQueueDepth, maxDelay, ctx, timers))
      )
      .narrow


  class RequestId

  sealed trait LimitActorCommand

  case class RequestReceived(sender: ActorRef[LimitActorResponse], request: HttpRequest) extends LimitActorCommand {
    val id = new RequestId
  }

  case class Replied(startTime: Long, duration: Long, didDrop: Boolean, id: RequestId) extends LimitActorCommand
  case class Ignore(id: RequestId) extends LimitActorCommand
  case class RequestTimedOut(startTime: Long, id: RequestId) extends LimitActorCommand
  case class MaxRequestDelayPassed(sender: ActorRef[LimitActorResponse], id: RequestId) extends LimitActorCommand

  sealed trait LimitActorResponse

  class RequestAccepted(val request: HttpRequest, actor: ActorRef[LimitActorCommand], clock: () => Long, id: RequestId)
    extends LimitActorResponse {

    val startTime: Long = clock()

    def success(): Unit = actor ! Replied(startTime, clock() - startTime, didDrop = false, id)

    def dropped(): Unit = actor ! Replied(startTime, clock() - startTime, didDrop = true, id)

    def ignore(): Unit = actor ! Ignore(id)
  }

  case object RequestRejected extends LimitActorResponse
}

class LimitActor {

}
