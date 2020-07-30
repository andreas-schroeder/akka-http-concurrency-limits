package akka.http.concurrency.limits

import akka.NotUsed
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, Scheduler}
import akka.http.concurrency.limits.GlobalLimitBidiFlow.{InFlight, Lease, LimitShape, Rejected}
import akka.http.concurrency.limits.LimitActor._
import akka.http.concurrency.limits.LimitBidiFolow.{Dropped, Ignored, Outcome, Processed}
import akka.stream.impl.Buffer
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage._
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.util.{OptionVal, Timeout}

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object GlobalLimitBidiFlow {
  def apply[In, Out](limiter: ActorRef[LimitActorCommand],
                     parallelism: Int,
                     timeout: FiniteDuration,
                     weight: Out => Int,
                     rejectionResponse: In => Out,
                     result: Out => Outcome = (_: Out) => Processed): BidiFlow[In, In, Out, Out, NotUsed] =
    BidiFlow.fromGraph(new GlobalLimitBidi(limiter, parallelism, timeout, weight, rejectionResponse, result))

  type LimitShape[In, Out] = BidiShape[In, In, Out, Out]

  final class InFlight(val startTime: Long, val lease: Lease)

  final class Lease(var amount: Int, val deadline: Long, val id: Id)

  final class Rejected(var amount: Int, val deadline: Long)

  final case class LeaseExpired(deadline: Long)
}

class GlobalLimitBidi[In, Out](limiter: ActorRef[LimitActorCommand],
                               parallelism: Int,
                               timeout: FiniteDuration,
                               weight: Out => Int,
                               rejectionResponse: In => Out,
                               result: Out => Outcome,
                               clock: () => Long = () => System.nanoTime())
    extends GraphStage[LimitShape[In, Out]] {

  private val in = Inlet[In]("LimitBidiFlow.in")
  private val toWrapped = Outlet[In]("LimitBidiFlow.toWrapped")
  private val fromWrapped = Inlet[Out]("LimitBidiFlow.fromWrapped")
  private val out = Outlet[Out]("LimitBidiFlow.out")

  def shape: LimitShape[In, Out] =
    BidiShape(in, toWrapped, fromWrapped, out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) {

    var pending: Boolean = false
    var pendingElement: In = _
    var inFlightElements: Buffer[InFlight] = _
    var leases: Buffer[Lease] = _
    var rejection: OptionVal[Rejected] = OptionVal.None
    var pullSuppressed = false

    implicit val askTimeout: Timeout = Timeout(timeout)
    implicit var scheduler: Scheduler = _
    implicit var ec: ExecutionContext = _

    override def preStart(): Unit = {
      val system = materializer.system
      scheduler = system.toTyped.scheduler
      ec = system.dispatcher
      inFlightElements = Buffer(parallelism, inheritedAttributes)
      leases = Buffer(parallelism, inheritedAttributes)
    }

    setHandler(
      in,
      new InHandler {
        def onPush(): Unit = {
          pendingElement = grab(in)
          pending = true

          val maybeLease = immediateAccept()
          if (maybeLease.isDefined) accept(maybeLease.get)
          else if (immediateReject()) reject()
          else askLimiter()
        }

        override def onUpstreamFinish(): Unit = complete(toWrapped)
      }
    )

    @tailrec
    private def immediateAccept(): OptionVal[Lease] = {
      if (leases.used > 0) {
        val lease = leases.dequeue()
        if (lease.deadline < clock()) {
          immediateAccept() // lease expired, try once more.
        } else {
          lease.amount -= 1
          OptionVal.Some(lease)
        }
      } else OptionVal.None
    }

    private def immediateReject(): Boolean = {
      if (!rejection.isDefined) {
        false
      } else {
        val lease = rejection.get
        if (lease.deadline < clock()) {
          rejection = OptionVal.None
          false
        } else {
          lease.amount -= 1
          if (lease.amount == 0) rejection = OptionVal.None
          true
        }
      }
    }

    def accept(lease: Lease): Unit = {
      push(toWrapped, pendingElement)
      inFlightElements.enqueue(new InFlight(clock(), lease))
      pending = false
    }

    def reject(): Unit = {
      push(out, rejectionResponse(pendingElement))
      pending = false
    }

    def askLimiter(): Unit = {
      pending = true
      limiter.ask[LimitActorResponse](sender => RequestCapacity(sender)).onComplete(limiterResponse.invoke)
    }

    private val limiterResponse = createAsyncCallback[Try[LimitActorResponse]] {
      case Success(CapacityGranted(amount, deadline, id)) =>
        val lease = new Lease(amount, deadline, id)
        if (pending) accept(lease) else leases.enqueue(lease)
        scheduleOnce(id, deadline.nanos)

      case Success(CapacityRejected(amount, deadline)) =>
        if (amount == 1 && pending) {
          reject()
        } else {
          val r = new Rejected(amount, deadline)
          rejection = OptionVal.Some(r)
          if (pending) {
            r.amount -= 1
            reject()
          }
        }

      case Failure(ex) => failStage(ex)
    }

    setHandler(toWrapped, new OutHandler {
      override def onPull(): Unit =
        if (inFlightElements.used < inFlightElements.capacity) pull(in) else pullSuppressed = true

      override def onDownstreamFinish(cause: Throwable): Unit = cancel(in, cause)
    })

    setHandler(fromWrapped, new InHandler {
      override def onPush(): Unit = {
        val response = grab(fromWrapped)
        push(out, response)
        val inFlight = inFlightElements.dequeue()
        recordResponse(inFlight, response)
        pullNowIfSuppressed()
        returnOrRelease(inFlight.lease)
        maybeAcceptPending()
      }
    })

    private def maybeAcceptPending(): Unit = if (pending) {
      val maybeLease = immediateAccept()
      if (maybeLease.isDefined) accept(maybeLease.get)
    }

    private def recordResponse(inflight: InFlight, response: Out): Unit = {
      import inflight._
      val command = result(response) match {
        case Processed => Replied(startTime, clock() - startTime, didDrop = false, weight(response))
        case Dropped   => Replied(startTime, clock() - startTime, didDrop = true, weight(response))
        case Ignored   => Ignore
      }
      limiter ! command
    }

    private def returnOrRelease(lease: Lease): Unit = {
      if (lease.amount == 0 || lease.deadline <= clock()) {
        limiter ! ReleaseCapacityGrant(lease.id)
      } else {
        leases.enqueue(lease)
      }
    }

    private def pullNowIfSuppressed(): Unit = if (pullSuppressed) {
      tryPull(in)
      pullSuppressed = false
    }

    setHandler(out, new OutHandler {
      // when the last element was rejected, 'fromWrapped' is already pulled and
      // 'in' isn't pulled. So we need to pull the right inlet.
      override def onPull(): Unit = if (hasBeenPulled(fromWrapped)) pull(in) else pull(fromWrapped)
      override def onDownstreamFinish(cause: Throwable): Unit = cancel(fromWrapped, cause)
    })

    override protected def onTimer(timerKey: Any): Unit = timerKey match {
      case id: Id =>
        // otherwise, lease is in use and will be returned as soon as it's released
        if (dropLease(id)) limiter ! ReleaseCapacityGrant(id)
      case _ => ()
    }

    private def dropLease(id: Id): Boolean = {
      // just to make sure we don't loop infinitely, one pass over leases is enough.
      var i = leases.used
      var found = false
      while (i > 0) {
        val lease = leases.dequeue()
        if (lease.id == id) {
          found = true
        } else {
          leases.enqueue(lease)
        }
        i -= 1
      }
      found
    }

    override def postStop(): Unit = {
      // Akka Http tears down the request handler flow on request timeout, and therefore
      // any in-flight request at this time was dropped.
      while (inFlightElements.nonEmpty) {
        val elem = inFlightElements.dequeue()
        limiter ! Replied(elem.startTime, clock() - elem.startTime, didDrop = true, 1)
      }
      while (leases.nonEmpty) {
        limiter ! ReleaseCapacityGrant(leases.dequeue().id)
      }
    }
  }
}
