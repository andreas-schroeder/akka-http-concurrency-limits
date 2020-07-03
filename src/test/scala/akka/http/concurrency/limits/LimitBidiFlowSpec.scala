package akka.http.concurrency.limits

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor.testkit.typed.scaladsl.{ManualTime, ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.scaladsl.adapter._
import akka.http.concurrency.limits.LimitActor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.pattern
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import com.netflix.concurrency.limits.limit.SettableLimit
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

object LimitBidiFlowSpec {
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
class LimitBidiFlowSpec extends ScalaTestWithActorTestKit(LimitBidiFlowSpec.config) with AnyWordSpecLike {

  val manualTime: ManualTime = ManualTime()

  "LimitBidiFlow" should {
    "pull from response-in when initial and response-out is pulled" in new ConnectedLimitBidiFlow {
      responseOut.request(1)
      responseIn.expectRequest() shouldBe 1L
    }

    "pull from request-in when request-out is pulled" in new ConnectedLimitBidiFlow {
      requestOut.request(1)
      requestIn.expectRequest() shouldBe 1L
    }

    "ask limiter actor when request-in is pushed" in new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      probe.expectMessageType[RequestReceived]
    }

    "reject request when limiter actor replies with reject" in new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      val received = probe.expectMessageType[RequestReceived]
      received.sender ! RequestRejected

      responseOut.expectNext().status shouldBe StatusCodes.TooManyRequests
    }

    "forward request when limiter actor replies with accept" in new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      acceptRequest()

      requestOut.expectNext()
    }

    "fail when limiter actor doesn't reply in time" in new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      manualTime.timePasses(3.seconds)

      requestOut.expectError()
      responseOut.expectError()
    }

    "forward response when response-in is pushed" in new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      acceptRequest()
      val response = HttpResponse()
      responseIn.sendNext(response)
      responseOut.expectNext() shouldBe response
    }

    "measure latency of responses" in new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      val replyProbe = acceptRequest(5L, 50L)
      responseIn.sendNext(HttpResponse())

      val replied = replyProbe.expectMessageType[Replied]
      replied.startTime shouldBe 5L
      replied.duration shouldBe 45L
      replied.didDrop shouldBe false
    }

    "ignore latency of client errors" in new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      val replyProbe = acceptRequest()
      responseIn.sendNext(HttpResponse(status = StatusCodes.Forbidden))

      replyProbe.expectMessageType[Ignore]
    }

    "ignore latency of server errors" in new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      val replyProbe = acceptRequest()
      responseIn.sendNext(HttpResponse(status = StatusCodes.InternalServerError))

      replyProbe.expectMessageType[Ignore]
    }

    "announce request drop if stage stops (e.g. due to http server time-out)" in  new PulledLimitBidiFlow {
      requestIn.sendNext(request())
      val replyProbe = acceptRequest()

      responseOut.cancel() // http server blueprint will cancel outlet on timeout

      val replied = replyProbe.expectMessageType[Replied]
      replied.didDrop shouldBe true
    }

    "report responses of pipelined requests in order" in new PulledLimitBidiFlow {

      // first request pulled
      requestIn.sendNext(request())
      val replyProbe1 = acceptRequest()
      requestOut.expectNext()

      // second request pulled before first one completes
      requestOut.request(1)
      requestIn.sendNext(request())
      val replyProbe2 = acceptRequest()

      // first response provided
      responseIn.sendNext(HttpResponse())
      responseOut.expectNext()
      replyProbe1.expectMessageType[Replied]

      // second response provided
      responseOut.request(1)
      responseIn.sendNext(HttpResponse())
      replyProbe2.expectMessageType[Replied]
    }
  }

  def request(path: String = "/"): HttpRequest =
    akka.http.scaladsl.client.RequestBuilding.Get(path)

  //noinspection TypeAnnotation
  trait ConnectedLimitBidiFlow {
    implicit val sys = system.toClassic
    implicit val mat = Materializer(sys)
    val requestIn = TestPublisher.probe[HttpRequest]()
    val requestOut = TestSubscriber.probe[HttpRequest]()
    val responseIn = TestPublisher.probe[HttpResponse]()
    val responseOut = TestSubscriber.probe[HttpResponse]()
    val probe = TestProbe[RequestReceived]()

    val testSetup = LimitBidiFlow(probe.ref) join Flow.fromSinkAndSource(
      Sink.fromSubscriber(requestOut),
      Source.fromPublisher(responseIn)
    )

    Source.fromPublisher(requestIn).via(testSetup).runWith(Sink.fromSubscriber(responseOut))

    responseOut.ensureSubscription()
    responseIn.ensureSubscription()
    requestOut.ensureSubscription()
    requestIn.ensureSubscription()
  }

  trait PulledLimitBidiFlow extends ConnectedLimitBidiFlow {
    responseOut.request(1)
    requestOut.request(1)

    def acceptRequest(startTime: Long = 1L, replyTime: Long = 2L): TestProbe[LimitActorCommand] = {
      val clock = {
        val first = new AtomicBoolean(true)
        () =>
          if (first.get()) {
            first.set(false)
            startTime
          } else replyTime
      }

      val received = probe.expectMessageType[RequestReceived]
      val replyProbe = TestProbe[LimitActorCommand]()
      received.sender ! new RequestAccepted(received.request, replyProbe.ref, clock, received.id)
      replyProbe
    }
  }

  "it" should {
    "limit" ignore {

      val limit = new SettableLimit(5)

      val limiter = system.systemActorOf(LimitActor.liFoQueued(limit, 10, _ => 100.millis), "limiter")

      val limitFlow = LimitBidiFlow(limiter)

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
}
