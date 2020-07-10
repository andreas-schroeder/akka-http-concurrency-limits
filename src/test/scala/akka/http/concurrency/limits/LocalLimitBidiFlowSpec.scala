package akka.http.concurrency.limits

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.testkit.typed.scaladsl.{ManualTime, ScalaTestWithActorTestKit}
import akka.actor.typed.scaladsl.adapter._
import akka.http.concurrency.limits.LimitBidiFolow._
import akka.stream.Materializer
import akka.stream.scaladsl.{BidiFlow, Flow, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import com.netflix.concurrency.limits.Limit
import com.netflix.concurrency.limits.limit.SettableLimit
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AnyWordSpecLike

object LocalLimitBidiFlowSpec {
  val config: String =
    s"""
       |akka.scheduler.implementation = "akka.testkit.ExplicitlyTriggeredScheduler"
       |akka.stream.materializer.initial-input-buffer-size = 1
       |akka.stream.materializer.max-input-buffer-size = 1
       |akka.http.server.request-timeout = 2s
       |akka.actor.default-dispatcher = { type = "akka.testkit.CallingThreadDispatcherConfigurator" }
       |""".stripMargin
}

//noinspection TypeAnnotation
class LocalLimitBidiFlowSpec extends ScalaTestWithActorTestKit(LocalLimitBidiFlowSpec.config) with AnyWordSpecLike with MockitoSugar with ArgumentMatchersSugar {

  val manualTime: ManualTime = ManualTime()

  "LimitBidiFlow" should {
    "pull from response-in when initial and response-out is pulled" in new ConnectedLimitBidiFlow {
      start()
      responseOut.request(1)
      responseIn.expectRequest() shouldBe 1L
    }

    "pull from request-in when request-out is pulled" in new ConnectedLimitBidiFlow {
      start()
      requestOut.request(1)
      requestIn.expectRequest() shouldBe 1L
    }

    "pass through elements when request-in is pushed and limit not reached" in new PulledLimitBidiFlow {
      start()
      requestIn.sendNext("One")
      requestOut.expectNext() shouldBe "One"
    }

    "reject request when limiter actor replies with reject" in new PulledLimitBidiFlow {
      start(new SettableLimit(0))
      requestIn.sendNext("One")

      responseOut.expectNext() shouldBe "Rejected One"
    }

    "forward response when response-in is pushed" in new PulledLimitBidiFlow {
      start()
      requestIn.sendNext("One")
      responseIn.sendNext("Got One")
      responseOut.expectNext() shouldBe "Got One"
    }

    "measure latency of responses" in new PulledLimitBidiFlow {
      val limit = mockLimitWithCapacity(1)
      start(limit, 5L, 50L)
      requestIn.sendNext("One")
      responseIn.sendNext("Got One")

      verify(limit).onSample(5L, 45L, 0, false)
    }

    "ignore latency when verdict is Ignore" in new PulledLimitBidiFlow {
      val limit = mockLimitWithCapacity(1)
      start(limit)
      requestIn.sendNext("One")
      responseIn.sendNext("Ignore")

      verify(limit, never).onSample(any, any, any, any)
    }

    "report responses of pipelined requests in order" in new PulledLimitBidiFlow {
      val limit = mockLimitWithCapacity(2)
      start(limit)

      // first request pulled
      requestIn.sendNext("One")
      requestOut.expectNext()

      // second request pulled before first one completes
      requestOut.request(1)
      requestIn.sendNext("Two")

      // first response provided
      responseIn.sendNext("Got One")
      verify(limit).onSample(1L, 1L, 1, false)

      // second response provided
      responseOut.request(1)
      responseIn.sendNext("Got Two")
      verify(limit).onSample(2L, 0L, 0, false)
    }
  }

  def mockLimitWithCapacity(capacity: Int) = {
    val limit = mock[Limit]
    when(limit.getLimit).thenReturn(capacity)
    limit
  }


  //noinspection TypeAnnotation
  trait ConnectedLimitBidiFlow {
    implicit val sys = system.toClassic
    implicit val mat = Materializer(sys)
    val requestIn = TestPublisher.probe[String]()
    val requestOut = TestSubscriber.probe[String]()
    val responseIn = TestPublisher.probe[String]()
    val responseOut = TestSubscriber.probe[String]()

    val verdict: String => Outcome = {
      case "Ignore" => Ignored
      case "Drop"   => Dropped
      case _        => Processed
    }

    def start(limit: Limit = new SettableLimit(1), startTime: Long = 1L, replyTime: Long = 2L) = {
      val clock = {
        val first = new AtomicBoolean(true)
        () =>
          if (first.get()) {
            first.set(false)
            startTime
          } else replyTime
      }

      val testSetup = BidiFlow.fromGraph(
        new LocalLimitBidi[String, String](() => limit, s => s"Rejected $s", verdict, 2, clock)
      ) join Flow
        .fromSinkAndSource(Sink.fromSubscriber(requestOut), Source.fromPublisher(responseIn))

      Source.fromPublisher(requestIn).via(testSetup).runWith(Sink.fromSubscriber(responseOut))

      responseOut.ensureSubscription()
      responseIn.ensureSubscription()
      requestOut.ensureSubscription()
      requestIn.ensureSubscription()
    }
  }

  trait PulledLimitBidiFlow extends ConnectedLimitBidiFlow {

    override def start(limit: Limit = new SettableLimit(1), startTime: Long = 1L, replyTime: Long = 2L): Unit = {
      super.start(limit, startTime, replyTime)
      responseOut.request(1)
      requestOut.request(1)
    }
  }
}
