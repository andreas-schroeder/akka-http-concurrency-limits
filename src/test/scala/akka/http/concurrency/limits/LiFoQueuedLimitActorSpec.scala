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
      |akka.http.server.request-timeout = 2s
      |akka.actor.default-dispatcher = { type = "akka.testkit.CallingThreadDispatcherConfigurator" }
      |""".stripMargin
}

class LiFoQueuedLimitActorSpec extends ScalaTestWithActorTestKit(LiFoQueuedLimitActorSpec.config) with AnyWordSpecLike {

  val manualTime: ManualTime = ManualTime()

  def spawnActorAndProbe(
    limit: Limit = new SettableLimit(1),
    maxQueueDepth: Int = 1,
    maxDelay: String => FiniteDuration = _ => 0.millis
  ): (ActorRef[Element[String]], TestProbe[LimitActorResponse[String]]) =
    (spawn(LimitActor.liFoQueued(limit, maxQueueDepth, maxDelay)), TestProbe[LimitActorResponse[String]]())

  def element(probe: TestProbe[LimitActorResponse[String]],
              value: String = "One",
              startTime: Long = 1L): Element[String] =
    Element(probe.ref, value, startTime)

  "LiFoQueuedLimitActor" should {

    "accept requests when below limit" in {
      val (actor, probe: TestProbe[LimitActorResponse[String]]) = spawnActorAndProbe()

      actor ! element(probe)

      probe.expectMessageType[ElementAccepted[String]]
    }

    "queue requests when above concurrency limit and below queue capacity limit" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = _ => 100.millis)

      actor ! element(probe)

      probe.receiveMessage()

      actor ! element(probe)

      probe.expectNoMessage(10.millis) // no immediate rejection.
    }

    "immediately reject requests when above concurrency limit and above queue capacity limit" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = _ => 100.millis)

      actor ! element(probe) // accepted ...

      probe.receiveMessage()

      actor ! element(probe) // queued ...

      actor ! element(probe) // rejected.

      probe.expectMessageType[ElementRejected[String]]
    }

    "immediately reject requests when above concurrency limit and request has no delay time" in {
      val (actor, probe) = spawnActorAndProbe(new SettableLimit(0))

      actor ! element(probe)

      probe.expectMessageType[ElementRejected[String]]
    }

    "eventually reject queued requests when max delay time exceeded" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = _ => 100.millis)

      actor ! element(probe) // accepted ...

      probe.receiveMessage()

      actor ! element(probe) // queued ...

      manualTime.timePasses(110.millis)

      probe.expectMessageType[ElementRejected[String]]
    }

    "eventually accept queued requests when concurrency limit is undershot" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = _ => 500.millis)

      actor ! element(probe) // accepted ...

      val accepted = probe.expectMessageType[ElementAccepted[String]]

      actor ! element(probe) // queued ...

      accepted.success(5L) // complete first request

      probe.expectMessageType[ElementAccepted[String]]
    }

    "accept queued requests in LiFo order" in {
      val (actor, probe) = spawnActorAndProbe(maxQueueDepth = 3, maxDelay = _ => 500.millis)

      actor ! element(probe) // accepted ...

      val accepted = probe.expectMessageType[ElementAccepted[String]]

      actor ! element(probe) // queued ...

      actor ! element(probe, "Two") // queued ...

      accepted.success(5L) // complete first request

      val accepted2 = probe.expectMessageType[ElementAccepted[String]]

      accepted2.value shouldBe "Two" // second element is returned first.
    }

    "eventually timeout accepted requests when response does not arrive in time" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! element(probe)

      probe.expectMessageType[ElementAccepted[String]]

      manualTime.timePasses(3.seconds) // based on Akka Http server timeout (set to 2 seconds)

      limit.dropped shouldBe 1

      actor ! element(probe)

      probe.expectMessageType[ElementAccepted[String]] // next request gets accepted
    }

    "ignore response after accepted request timed out" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! element(probe)

      val accepted = probe.expectMessageType[ElementAccepted[String]]

      manualTime.timePasses(3.seconds) // based on Akka Http server timeout (set to 2 seconds)

      accepted.success(5L)

      limit.dropped shouldBe 1
      limit.success shouldBe 0
    }

    "forward successful response outcome to limit algorithm" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! element(probe)

      probe.expectMessageType[ElementAccepted[String]].success(5L)

      limit.dropped shouldBe 0
      limit.success shouldBe 1
    }

    "forward dropped response outcome to limit algorithm" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! element(probe)

      probe.expectMessageType[ElementAccepted[String]].dropped(5L)

      limit.dropped shouldBe 1
      limit.success shouldBe 0
    }

    "skip ignore response outcomes" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! element(probe)

      probe.expectMessageType[ElementAccepted[String]].ignore()

      limit.dropped shouldBe 0
      limit.success shouldBe 0
    }

    "adjust limit when limit algorithm increases limit" in {
      val limit = new SettableLimit(0)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! element(probe)

      probe.expectMessageType[ElementRejected[String]] // over capacity

      limit.setLimit(1) // capacity increases

      actor ! element(probe)

      probe.expectMessageType[ElementAccepted[String]] // now within capacity
    }

    "adjust limit when limit algorithm decreases limit" in {
      val limit = new SettableLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! element(probe)

      val accept = probe.expectMessageType[ElementAccepted[String]] // within capacity

      limit.setLimit(0) // capacity decreases

      actor ! element(probe)

      accept.success(5L)

      probe.expectMessageType[ElementRejected[String]] // still over capacity
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
