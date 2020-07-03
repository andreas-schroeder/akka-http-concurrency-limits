package akka.http.concurrency.limits

import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.testkit.typed.scaladsl.ManualTime
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.http.concurrency.limits.LimitActor._
import akka.http.scaladsl.model.HttpRequest
import com.netflix.concurrency.limits.Limit
import com.netflix.concurrency.limits.limit.SettableLimit
import com.typesafe.config.{Config, ConfigFactory}

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
    maxDelay: HttpRequest => FiniteDuration = _ => 0.millis
  ): (ActorRef[RequestReceived], TestProbe[LimitActorResponse]) =
    (spawn(LimitActor.liFoQueued(limit, maxQueueDepth, maxDelay)), TestProbe[LimitActorResponse]())

  def requestReceived(probe: TestProbe[LimitActorResponse], path: String = "/"): RequestReceived =
    RequestReceived(probe.ref, akka.http.scaladsl.client.RequestBuilding.Get(path))

  "LiFoQueuedLimitActor" should {

    "accept requests when below limit" in {
      val (actor, probe: TestProbe[LimitActorResponse]) = spawnActorAndProbe()

      actor ! requestReceived(probe)

      probe.expectMessageType[RequestAccepted]
    }

    "queue requests when above concurrency limit and below queue capacity limit" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = _ => 100.millis)

      actor ! requestReceived(probe)

      probe.receiveMessage()

      actor ! requestReceived(probe)

      probe.expectNoMessage(10.millis) // no immediate rejection.
    }

    "immediately reject requests when above concurrency limit and above queue capacity limit" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = _ => 100.millis)

      actor ! requestReceived(probe) // accepted ...

      probe.receiveMessage()

      actor ! requestReceived(probe) // queued ...

      actor ! requestReceived(probe) // rejected.

      probe.expectMessage(RequestRejected)
    }

    "immediately reject requests when above concurrency limit and request has no delay time" in {
      val (actor, probe) = spawnActorAndProbe(new SettableLimit(0))

      actor ! requestReceived(probe)

      probe.expectMessage(RequestRejected)
    }

    "eventually reject queued requests when max delay time exceeded" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = _ => 100.millis)

      actor ! requestReceived(probe) // accepted ...

      probe.receiveMessage()

      actor ! requestReceived(probe) // queued ...

      manualTime.timePasses(110.millis)

      probe.expectMessage(RequestRejected)
    }

    "eventually accept queued requests when concurrency limit is undershot" in {
      val (actor, probe) = spawnActorAndProbe(maxDelay = _ => 500.millis)

      actor ! requestReceived(probe) // accepted ...

      val accepted = probe.expectMessageType[RequestAccepted]

      actor ! requestReceived(probe) // queued ...

      accepted.success() // complete first request

      probe.expectMessageType[RequestAccepted]
    }

    "accept queued requests in LiFo order" in {
      val (actor, probe) = spawnActorAndProbe(maxQueueDepth = 3, maxDelay = _ => 500.millis)

      actor ! requestReceived(probe) // accepted ...

      val accepted = probe.expectMessageType[RequestAccepted]

      actor ! requestReceived(probe, "/1") // queued ...

      actor ! requestReceived(probe, "/2") // queued ...

      accepted.success() // complete first request

      val accepted2 = probe.expectMessageType[RequestAccepted]

      accepted2.request.uri.path.toString shouldBe "/2"
    }

    "eventually timeout accepted requests when response does not arrive in time" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! requestReceived(probe)

      probe.expectMessageType[RequestAccepted]

      manualTime.timePasses(3.seconds) // based on Akka Http server timeout (set to 2 seconds)

      limit.dropped shouldBe 1

      actor ! requestReceived(probe)

      probe.expectMessageType[RequestAccepted] // next request gets accepted
    }

    "ignore response after accepted request timed out" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! requestReceived(probe)

      val accepted = probe.expectMessageType[RequestAccepted]

      manualTime.timePasses(3.seconds) // based on Akka Http server timeout (set to 2 seconds)

      accepted.success()

      limit.dropped shouldBe 1
      limit.success shouldBe 0
    }

    "forward successful response outcome to limit algorithm" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! requestReceived(probe)

      probe.expectMessageType[RequestAccepted].success()

      limit.dropped shouldBe 0
      limit.success shouldBe 1
    }

    "forward dropped response outcome to limit algorithm" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! requestReceived(probe)

      probe.expectMessageType[RequestAccepted].dropped()

      limit.dropped shouldBe 1
      limit.success shouldBe 0
    }

    "skip ignore response outcomes" in {
      val limit = new TestLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! requestReceived(probe)

      probe.expectMessageType[RequestAccepted].ignore()

      limit.dropped shouldBe 0
      limit.success shouldBe 0
    }

    "adjust limit when limit algorithm increases limit" in {
      val limit = new SettableLimit(0)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! requestReceived(probe)

      probe.expectMessage(RequestRejected) // over capacity

      limit.setLimit(1) // capacity increases

      actor ! requestReceived(probe)

      probe.expectMessageType[RequestAccepted] // now within capacity
    }

    "adjust limit when limit algorithm decreases limit" in {
      val limit = new SettableLimit(1)
      val (actor, probe) = spawnActorAndProbe(limit)

      actor ! requestReceived(probe)

      val accept = probe.expectMessageType[RequestAccepted] // within capacity

      limit.setLimit(0) // capacity decreases

      actor ! requestReceived(probe)

      accept.success()

      probe.expectMessage(RequestRejected) // still over capacity
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
