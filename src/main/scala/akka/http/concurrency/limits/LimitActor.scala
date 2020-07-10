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
  def liFoQueued[T](limitAlgorithm: Limit,
            maxLiFoQueueDepth: Int,
            maxDelay: T => FiniteDuration = (_: T) => 0.nanos): Behavior[Element[T]] =
    Behaviors
      .setup[LimitActorCommand[T]](
        ctx =>
          Behaviors
            .withTimers(timers => new LiFoQueuedLimitActor(limitAlgorithm, maxLiFoQueueDepth, maxDelay, ctx, timers))
      )
      .narrow


  sealed trait LimitActorCommand[T]

  case class Element[T](sender: ActorRef[LimitActorResponse[T]], value: T, startTime: Long) extends LimitActorCommand[T]

  case class Replied[T](startTime: Long, duration: Long, didDrop: Boolean, element: Element[T]) extends LimitActorCommand[T]
  case class Ignore[T](element: Element[T]) extends LimitActorCommand[T]
  case class ElementTimedOut[T](element: Element[T]) extends LimitActorCommand[T]
  case class MaxDelayPassed[T](sender: ActorRef[LimitActorResponse[T]], element: Element[T]) extends LimitActorCommand[T]

  sealed trait LimitActorResponse[T]

  class ElementAccepted[T](actor: ActorRef[LimitActorCommand[T]], element: Element[T])
    extends LimitActorResponse[T] {

    def value: T = element.value

    def startTime: Long = element.startTime

    def success(endTime: Long): Unit = actor ! Replied(startTime, endTime - startTime, didDrop = false, element)

    def dropped(endTime: Long): Unit = actor ! Replied(startTime, endTime - startTime, didDrop = true, element)

    def ignore(): Unit = actor ! Ignore(element)
  }

  case class ElementRejected[T](value: T) extends LimitActorResponse[T]
}