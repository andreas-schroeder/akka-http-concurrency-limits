package akka.http.concurrency.limits

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
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
                 batchSize: Int = 10,
                 batchTimeout: FiniteDuration = 500.millis,
                 maxDelay: FiniteDuration = 0.nanos,
                 timeout: FiniteDuration = 1.second): Behavior[LimitActorCommand] =
    Behaviors
      .setup[LimitActorMessage] { ctx =>
        Behaviors.withTimers { timers =>
          new LiFoQueuedLimitActor(
            limitAlgorithm,
            maxLiFoQueueDepth,
            batchSize,
            batchTimeout,
            maxDelay,
            timeout,
            ctx,
            timers
          )
        }
      }
      .narrow

  sealed trait LimitActorMessage
  sealed trait LimitActorCommand extends LimitActorMessage
  sealed trait InternalLimitActorMessage extends LimitActorMessage

  class Id

  case class RequestCapacity(sender: ActorRef[LimitActorResponse], id: Id = new Id) extends LimitActorCommand
  case class Replied(startTime: Long, duration: Long, didDrop: Boolean, weight: Int) extends LimitActorCommand
  case object Ignore extends LimitActorCommand
  case class ReleaseCapacityGrant(id: Id) extends LimitActorCommand

  case class MaxDelayPassed(request: RequestCapacity) extends InternalLimitActorMessage
  case class CapacityGrantTimedOut(id: Id) extends InternalLimitActorMessage

  sealed trait LimitActorResponse

  case class CapacityGranted(elements: Int, until: Long, id: Id) extends LimitActorResponse
  case class CapacityRejected(elements: Int, until: Long) extends LimitActorResponse

}
