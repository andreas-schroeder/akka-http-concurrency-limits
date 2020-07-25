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
                    maxDelay: FiniteDuration = 0.nanos,
                    timeout: FiniteDuration = 1.second): Behavior[Element] =
    Behaviors
      .setup[LimitActorCommand](
        ctx =>
          Behaviors
            .withTimers(
              timers => new LiFoQueuedLimitActor(limitAlgorithm, maxLiFoQueueDepth, maxDelay, timeout, ctx, timers)
          )
      )
      .narrow

  sealed trait LimitActorCommand

  case class Element(sender: ActorRef[LimitActorResponse], id: Id = new Id) extends LimitActorCommand

  final class Id

  case class Replied(startTime: Long, duration: Long, didDrop: Boolean, weight: Int, id: Id)
      extends LimitActorCommand
  case class Ignore(id: Id) extends LimitActorCommand
  case class ElementTimedOut(element: Element, startTime: Long) extends LimitActorCommand
  case class MaxDelayPassed(element: Element) extends LimitActorCommand

  sealed trait LimitActorResponse

  class ElementAccepted(actor: ActorRef[LimitActorCommand], id: Id) extends LimitActorResponse {

    def success(startTime: Long, endTime: Long, weight: Int): Unit =
      actor ! Replied(startTime, endTime - startTime, didDrop = false, weight, id)

    def dropped(startTime: Long, endTime: Long, weight: Int): Unit =
      actor ! Replied(startTime, endTime - startTime, didDrop = true, weight, id)

    def ignore(): Unit = actor ! Ignore(id)
  }

  case object ElementRejected extends LimitActorResponse
}
