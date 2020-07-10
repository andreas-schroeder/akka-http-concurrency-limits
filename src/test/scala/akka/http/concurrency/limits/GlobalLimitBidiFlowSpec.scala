package akka.http.concurrency.limits

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.testkit.typed.scaladsl.{ManualTime, ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.scaladsl.adapter._
import akka.http.concurrency.limits.LimitActor._
import akka.http.concurrency.limits.LimitBidiFolow.{Dropped, Ignored, Outcome, Processed}
import akka.stream.Materializer
import akka.stream.scaladsl.{BidiFlow, Flow, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

object GlobalLimitBidiFlowSpec {
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
class GlobalLimitBidiFlowSpec extends ScalaTestWithActorTestKit(GlobalLimitBidiFlowSpec.config) with AnyWordSpecLike {

  val manualTime: ManualTime = ManualTime()

  "LimitBidiFlow" should {
    "pull from response-in when initial and response-out is pulled" in new ConnectedLimitBidiFlow {
      start()
      out.request(1)
      fromWrapped.expectRequest() shouldBe 1L
    }

    "pull from request-in when request-out is pulled" in new ConnectedLimitBidiFlow {
      start()
      toWrapped.request(1)
      in.expectRequest() shouldBe 1L
    }

    "ask limiter actor when request-in is pushed" in new PulledLimitBidiFlow {
      println("INVOKING START")
      start()
      println("INVOKING START DONE")
      in.sendNext("One")
      probe.expectMessageType[Element[String]]
    }

    "reject request when limiter actor replies with reject" in new PulledLimitBidiFlow {
      start()
      in.sendNext("One")
      val received = probe.expectMessageType[Element[String]]
      received.sender ! ElementRejected("One")

      out.expectNext() shouldBe "Rejected One"
    }

    "forward request when limiter actor replies with accept" in new PulledLimitBidiFlow {
      start()
      in.sendNext("One")
      acceptRequest()

      toWrapped.expectNext() shouldBe "One"
    }

    "fail when limiter actor doesn't reply in time" in new PulledLimitBidiFlow {
      start()
      in.sendNext("One")
      manualTime.timePasses(3.seconds)

      toWrapped.expectError()
      out.expectError()
    }

    "forward response when response-in is pushed" in new PulledLimitBidiFlow {
      start()
      in.sendNext("One")
      acceptRequest()
      fromWrapped.sendNext("One Response")
      out.expectNext() shouldBe "One Response"
    }

    "measure latency of responses" in new PulledLimitBidiFlow {
      start(5L, 50L)
      in.sendNext("One")
      val replyProbe = acceptRequest()
      fromWrapped.sendNext("One Response")

      val replied = replyProbe.expectMessageType[Replied[String]]
      replied.startTime shouldBe 5L
      replied.duration shouldBe 45L
      replied.didDrop shouldBe false
    }

    "ignore latency when outcome is Ignore" in new PulledLimitBidiFlow {
      start(5L, 50L)
      in.sendNext("One")
      val replyProbe = acceptRequest()
      fromWrapped.sendNext("Ignore")

      replyProbe.expectMessageType[Ignore[String]]
    }

    "Announce request drop if stage stops (e.g. due to stage completion)" in new PulledLimitBidiFlow {
      start()

      in.sendNext("One")
      val replyProbe = acceptRequest()

      // Stage completes (e.g. http server blueprint will cancel on request timeout)
      out.cancel()
      in.sendComplete()

      val replied = replyProbe.expectMessageType[Replied[String]]
      replied.didDrop shouldBe true
    }

    "report responses of pipelined requests in order" in new PulledLimitBidiFlow {
      start()

      // first request pulled
      in.sendNext("One")
      val replyProbe1 = acceptRequest()
      toWrapped.expectNext()

      // second request pulled before first one completes
      toWrapped.request(1)
      in.sendNext("Two")
      val replyProbe2 = acceptRequest()

      // first response provided
      fromWrapped.sendNext("One Response")
      out.expectNext()
      replyProbe1.expectMessageType[Replied[String]].element.value shouldBe "One"

      // second response provided
      out.request(1)
      fromWrapped.sendNext("Two Response")
      replyProbe2.expectMessageType[Replied[String]].element.value shouldBe "Two"
    }
  }

  //noinspection TypeAnnotation
  trait ConnectedLimitBidiFlow {
    implicit val sys = system.toClassic
    implicit val mat = Materializer(sys)
    val in = TestPublisher.probe[String]()
    val toWrapped = TestSubscriber.probe[String]()
    val fromWrapped = TestPublisher.probe[String]()
    val out = TestSubscriber.probe[String]()
    val probe = TestProbe[Element[String]]()

    def start(startTime: Long = 1L, replyTime: Long = 2L) = {
      val verdict: String => Outcome = {
        case "Ignore" => Ignored
        case "Drop"   => Dropped
        case _        => Processed
      }

      val clock = {
        val first = new AtomicBoolean(true)
        () =>
          if (first.get()) {
            first.set(false)
            startTime
          } else replyTime
      }

      val testSetup = BidiFlow.fromGraph(
        new GlobalLimitBidi[String, String](probe.ref, s => s"Rejected $s", verdict, 10, 2.seconds, clock)
      ) join Flow
        .fromSinkAndSource(Sink.fromSubscriber(toWrapped), Source.fromPublisher(fromWrapped))

      Source.fromPublisher(in).via(testSetup).runWith(Sink.fromSubscriber(out))

      out.ensureSubscription()
      fromWrapped.ensureSubscription()
      toWrapped.ensureSubscription()
      in.ensureSubscription()
    }
  }

  trait PulledLimitBidiFlow extends ConnectedLimitBidiFlow {

    override def start(startTime: Long, replyTime: Long): Unit = {
      super.start(startTime, replyTime)
      out.request(1)
      toWrapped.request(1)
    }

    def acceptRequest(): TestProbe[LimitActorCommand[String]] = {
      val received = probe.expectMessageType[Element[String]]
      val replyProbe = TestProbe[LimitActorCommand[String]]()
      received.sender ! new ElementAccepted(replyProbe.ref, received)
      replyProbe
    }
  }

  /*
  "it" should {
    "limit" ignore {

      val limit = new SettableLimit(5)

      val limiter = system.systemActorOf(LimitActor.liFoQueued(limit, 10, _ => 100.millis), "limiter")

      val limitFlow = GlobalLimitBidiFlow(limiter)

      val counter = new AtomicInteger

      implicit val sys = system.toClassic
      implicit val ec = sys.dispatcher
      implicit val mat = Materializer(sys)

      val routes = pathSingleSlash {
        get {
          val cnt = counter.incrementAndGet()
          val delay =
            if (cnt >= 210) {
              20.millis
            } else if (cnt >= 200) {
              3.seconds
            } else {
              20.millis
            }
          complete(pattern.after(delay, system.scheduler.toClassic)(Future.successful("Ok")))
        }
      }

      Await.ready(Http().bindAndHandle(limitFlow.join(routes), "0.0.0.0", 8080), 2.seconds)

      for (i <- 1 to 600) {
        Thread.sleep(20)
        val response = Http().singleRequest(Get("http://localhost:8080/"))
        println(s"$i - Request")

        response.andThen {
          case Success(r) => println(s"$i - ${r.status}")
          case Failure(t) => println(s"$i - failed: ${t.getMessage}")
        }
      }

      Thread.sleep(200000)
    }
  }
 */
}
