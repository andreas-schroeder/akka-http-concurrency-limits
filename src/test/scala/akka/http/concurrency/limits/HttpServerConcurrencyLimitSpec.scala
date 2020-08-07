package akka.http.concurrency.limits

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.netflix.concurrency.limits.limit.SettableLimit
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, blocking}

class HttpServerConcurrencyLimitSpec extends AnyWordSpec with Matchers {

  def withLimitedServer(limit: Int, route: Route)(body: ActorSystem => Unit): Unit = {
    implicit val system: ActorSystem = ActorSystem()

    val config = HttpLiFoQueuedConcurrencyLimitConfig(new SettableLimit(10))
    val limitFlow = HttpServerConcurrencyLimit.liFoQueued(config)

    Await.ready(Http().newServerAt("0.0.0.0", 8080).bindFlow(limitFlow join route), 2.seconds)

    try {
      body(system)
    } finally {
      system.terminate()
    }
  }

  "HttpServerConcurrencyLimit" should {
    "accept requests under limit" in {
      val routes = pathSingleSlash {
        get {
          complete("Ok")
        }
      }

      withLimitedServer(2, routes) { implicit system: ActorSystem =>
        val response = Await.result(Http().singleRequest(Get("http://localhost:8080/")), 2.seconds)
        response.status.intValue shouldBe 200
      }
    }

    "reject requests over limit" in {
      val routes = pathSingleSlash {
        get {
          complete(Future {
            blocking {
              Thread.sleep(100)
              "Ok"
            }
          })
        }
      }

      withLimitedServer(2, routes) { implicit system: ActorSystem =>
        val eventualResponseCodes = for (_ <- 1 to 16)
          yield Http().singleRequest(Get("http://localhost:8080/")).map(_.status.intValue)

        val responseCodes = Await.result(Future.sequence(eventualResponseCodes), 3.seconds)
        responseCodes.count(_ == 200) shouldBe >=(2)
        responseCodes.count(_ == 429) shouldBe >(0)
      }
    }
  }
}
