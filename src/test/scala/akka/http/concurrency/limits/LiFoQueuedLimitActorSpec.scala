package akka.http.concurrency.limits

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

import akka.actor.testkit.typed.scaladsl.{ManualTime, ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.ActorRef
import akka.http.concurrency.limits.LimitActor._
import com.netflix.concurrency.limits.Limit
import com.netflix.concurrency.limits.limit.SettableLimit
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

object LiFoQueuedLimitActorSpec {

  val config: String =
    """
      |akka.scheduler.implementation = "akka.testkit.ExplicitlyTriggeredScheduler"
      |akka.actor.default-dispatcher = { type = "akka.testkit.CallingThreadDispatcherConfigurator" }
      |""".stripMargin
}

class LiFoQueuedLimitActorSpec extends ScalaTestWithActorTestKit(LiFoQueuedLimitActorSpec.config) with AnyWordSpecLike {

  val manualTime: ManualTime = ManualTime()

  def spawnActorAndProbe(
    limit: Limit = new SettableLimit(1),
    maxQueueDepth: Int = 1,
    batchSize: Int = 1,
    batchTimeout: FiniteDuration = 0.millis,
    maxDelay: FiniteDuration = 0.millis,
    timeout: FiniteDuration = 2.seconds
  ): (ActorRef[LimitActorCommand], TestProbe[LimitActorResponse]) =
    (
      spawn(LimitActor.liFoQueued(limit, maxQueueDepth, batchSize, batchTimeout, maxDelay, timeout)),
      TestProbe[LimitActorResponse]()
    )

  def request(probe: TestProbe[LimitActorResponse]): RequestCapacity = RequestCapacity(probe.ref)

  def release(grant: CapacityGranted): ReleaseCapacityGrant = ReleaseCapacityGrant(grant.id)

  def replied(startTime: Long = 1, duration: Long = 5, didDrop: Boolean = false, weight: Int = 1): Replied =
    Replied(startTime, duration, didDrop, weight)

  "LiFoQueuedLimitActor" should {

    "accept requests when below limit" in {
      val (actor, probe: TestProbe[LimitActorResponse]) = spawnActorAndProbe()

      actor ! request(probe)

      probe.expectMessageType[CapacityGranted]
    }

    "queue requests when above concurrency limit and below queue capacity limit" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = 100.millis)

      actor ! request(probe)

      probe.receiveMessage()

      actor ! request(probe)

      probe.expectNoMessage(10.millis) // no immediate rejection.
    }

    "immediately reject requests when above concurrency limit and above queue capacity limit" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = 100.millis)

      actor ! request(probe) // accepted ...

      probe.receiveMessage()

      actor ! request(probe) // queued ...

      actor ! request(probe) // rejected.

      probe.expectMessageType[CapacityRejected]
    }

    "immediately reject requests when above concurrency limit and request has no delay time" in {
      val (actor, probe) = spawnActorAndProbe(new SettableLimit(0))

      actor ! request(probe)

      probe.expectMessageType[CapacityRejected]
    }

    "eventually reject queued requests when max delay time exceeded" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = 100.millis)

      actor ! request(probe) // accepted ...

      probe.receiveMessage()

      actor ! request(probe) // queued ...

      manualTime.timePasses(110.millis)

      probe.expectMessageType[CapacityRejected]
    }

    "eventually accept queued requests when concurrency limit is undershot" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = 500.millis)

      actor ! request(probe) // accepted ...

      val grant = probe.expectMessageType[CapacityGranted]

      actor ! request(probe) // queued ...

      actor ! release(grant) // release grant

      probe.expectMessageType[CapacityGranted]
    }

    "accept queued requests in LiFo order" in {
      val (actor, probe) = spawnActorAndProbe(maxQueueDepth = 3, maxDelay = 500.millis)

      val probe2 = TestProbe[LimitActorResponse]()

      actor ! request(probe) // accepted ...

      val grant = probe.expectMessageType[CapacityGranted]

      actor ! request(probe) // queued ...

      actor ! request(probe2) // queued ...

      actor ! release(grant) // release first grant

      probe2.expectMessageType[CapacityGranted] // second request is accepted first.
    }

    "eventually timeout accepted requests when response does not arrive in time" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! request(probe)

      probe.expectMessageType[CapacityGranted]

      manualTime.timePasses(3.seconds) // based on Akka Http server timeout (set to 2 seconds)

      limit.dropped shouldBe 1

      actor ! request(probe)

      probe.expectMessageType[CapacityGranted] // next request gets accepted
    }

    "forward successful response outcome to limit algorithm" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! request(probe)

      probe.expectMessageType[CapacityGranted]

      actor ! replied()

      limit.dropped shouldBe 0
      limit.success shouldBe 1
    }

    "consider weight when forwarding response outcome to limit algorithm" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! request(probe)

      probe.expectMessageType[CapacityGranted]

      actor ! replied(weight = 3)

      limit.dropped shouldBe 0
      limit.success shouldBe 3
    }

    "forward dropped response outcome to limit algorithm" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! request(probe)

      probe.expectMessageType[CapacityGranted]

      actor ! replied(didDrop = true)

      limit.dropped shouldBe 1
      limit.success shouldBe 0
    }

    "skip ignore response outcomes" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! request(probe)

      probe.expectMessageType[CapacityGranted]

      actor ! Ignore

      limit.dropped shouldBe 0
      limit.success shouldBe 0
    }

    "adjust limit when limit algorithm increases limit" in {
      val limit = new SettableLimit(0)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! request(probe)

      probe.expectMessageType[CapacityRejected] // over capacity

      limit.setLimit(1) // capacity increases

      actor ! request(probe)

      probe.expectMessageType[CapacityGranted] // now within capacity
    }

    "adjust limit when limit algorithm decreases limit" in {
      val limit = new SettableLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! request(probe)

      val accept = probe.expectMessageType[CapacityGranted] // within capacity

      limit.setLimit(0) // capacity decreases

      actor ! request(probe)

      actor ! replied()

      probe.expectMessageType[CapacityRejected] // still over capacity
    }
  }
}

class TestLimit(limit: Int) extends Limit {

  val droppedCounter = new AtomicInteger()
  val successCounter = new AtomicInteger()

  def dropped: Int = droppedCounter.get()
  def success: Int = successCounter.get()

  def getLimit: Int = limit
  def notifyOnChange(consumer: Consumer[Integer]): Unit = ()
  def onSample(startTime: Long, rtt: Long, inflight: Int, didDrop: Boolean): Unit =
    if (didDrop) droppedCounter.incrementAndGet() else successCounter.incrementAndGet()
}
