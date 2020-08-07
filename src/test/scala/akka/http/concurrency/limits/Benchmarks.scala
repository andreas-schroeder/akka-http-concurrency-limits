package akka.http.concurrency.limits

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Directives.{complete, get, pathSingleSlash}
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{BidiFlow, Flow, Sink, Source}
import com.netflix.concurrency.limits.limit.SettableLimit

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._

object Benchmarks {

  type LimitFlow = BidiFlow[HttpRequest, HttpRequest, HttpResponse, HttpResponse, NotUsed]

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    val config = HttpLiFoQueuedConcurrencyLimitConfig(new SettableLimit(256))
    val limitFlow: LimitFlow = HttpServerConcurrencyLimit.liFoQueued(config)

    val b1 = compareRoutes("immediate routes", immediateRoute, limitFlow)
    val b2 = compareRoutes("5ms delayed routes", delayedRoute(5.millis), limitFlow)

    renderResults(b1 ++ b2)
  }

  def compareRoutes(name: String, route: Route, limitFlow: LimitFlow)(implicit system: ActorSystem): Seq[BenchmarkResult] = {
    def run(route: Flow[HttpRequest, HttpResponse, NotUsed], name: String) = {
      val binding = Await.result(Http().newServerAt("0.0.0.0", 8080).bindFlow(route), 2.seconds)
      println(s"$name:")
      val b = benchmark(name, clientFlow(8080), 10, 30)
      Await.ready(binding.terminate(2.seconds), 3.seconds)
      b
    }

    val unlimited = run(route, s"Unlimited $name")
    val limited = run(limitFlow join route, s"Limited $name")
    Seq(unlimited, limited)
  }

  def clientFlow(port: Int)(implicit system: ActorSystem): Source[Int, NotUsed] = {
    Source
      .repeat(Get(s"http://localhost:$port/") -> ())
      .via(Http().cachedHostConnectionPool[Unit]("localhost", port))
      .map(t => if (t._1.isSuccess) 1 else 0)
  }

  def delayedRoute(delay: FiniteDuration)(implicit system: ActorSystem): Route = {
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    pathSingleSlash {
      get {
        complete(akka.pattern.after(delay, system.scheduler)(Future.successful("Ok")))
      }
    }
  }

  val immediateRoute: Route = pathSingleSlash {
    get {
      complete("Ok")
    }
  }

  case class BenchmarkResult(name: String, avg: Double, stdDev: Double, min: Int, max: Int)

  def renderResults(results: Seq[BenchmarkResult]): Unit = {
    val paddedNames = rightPad(results.map(_.name + ":"))

    results.zip(paddedNames).foreach {
      case (b, paddedName) =>
        import b._
        println(f"$paddedName $avg%.2f ops ± $stdDev%.2f ($min, $max)")
    }
  }

  def benchmark(name: String, flow: Source[Int, NotUsed], warmup: Int, benchmark: Int)(
    implicit system: ActorSystem
  ): BenchmarkResult = {
    val waitTime = (warmup + benchmark + 5).seconds
    def report(in: (Int, Long)): Int = {
      val (count, pass) = in
      if (pass < warmup) {
        val no = (pass + 1).toString
        println(s"${leftPad(no, 3)} warmup:    ${leftPad(count.toString, 6)} op/s")
      } else {
        val no = (pass + 1 - warmup).toString
        println(s"${leftPad(no, 3)} benchmark: ${leftPad(count.toString, 6)} op/s")
      }
      count
    }

    val run = flow
      .conflate(_ + _)
      .throttle(1, 1.second)
      .dropWhile(_ <= 1)
      .zipWithIndex
      .map(report)
      .drop(warmup)
      .take(benchmark)
      .runWith(Sink.collection)

    val measurements = Await.result(run, waitTime).toSeq

    val avg = (1.0 * measurements.sum) / measurements.size

    val min = measurements.min
    val max = measurements.max
    val stdDev = Math.sqrt(measurements.map(m => (avg - m) * (avg - m)).sum / measurements.size)

    println(f"Throughput: $avg%.2f ops ± $stdDev%.2f ($min, $max)")
    BenchmarkResult(name, avg, stdDev, min, max)
  }

  def rightPad(str: String, length: Int): String = str.padTo(length, ' ')

  def rightPad(strings: Seq[String]): Seq[String] = {
    val maxLength = strings.map(_.length).max
    strings.map(rightPad(_, maxLength))
  }

  def leftPad(str: String, length: Int): String = {
    val c = length - str.length
    if (c <= 0) str else " " * c + str
  }
}
