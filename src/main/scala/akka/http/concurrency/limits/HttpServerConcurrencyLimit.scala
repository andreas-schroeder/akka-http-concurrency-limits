package akka.http.concurrency.limits

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter._
import akka.http.concurrency.limits.LimitBidiFolow.{Dropped, Ignored, Outcome, Processed}
import akka.http.scaladsl.model.StatusCodes.{ClientError, ServerError}
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.BidiFlow
import com.netflix.concurrency.limits.Limit

import scala.concurrent.duration._

object HttpServerConcurrencyLimit {

  def liFoQueued(
    config: HttpConcurrencyLimitConfig
  )(implicit system: ActorSystem): BidiFlow[HttpRequest, HttpRequest, HttpResponse, HttpResponse, NotUsed] = {

    val typed = system.toTyped

    val limitActor: ActorRef[LimitActor.Element[HttpRequest]] =
      typed.systemActorOf(
        LimitActor.liFoQueued(config.limitAlgorithm, config.maxLiFoQueueDepth, config.maxDelay),
        config.name
      )
    GlobalLimitBidiFlow(limitActor, config.pipeliningLimit, config.reqestTimeout, config.response, config.result)
  }

  val TooManyRequestsResponse: HttpResponse = HttpResponse(StatusCodes.TooManyRequests, entity = "Too many requests")
}

final case class HttpConcurrencyLimitConfig(limitAlgorithm: Limit,
                                            maxLiFoQueueDepth: Int,
                                            name: String,
                                            pipeliningLimit: Int,
                                            reqestTimeout: FiniteDuration,
                                            maxDelay: HttpRequest => FiniteDuration,
                                            response: HttpRequest => HttpResponse,
                                            result: HttpResponse => Outcome)

object HttpConcurrencyLimitConfig {

  val DefaultResult: HttpResponse => Outcome = r =>
    r.status match {
      case ServerError(_) => Ignored
      case ClientError(_) => Ignored
      case _              => Processed
  }

  /**
   * Use this if server errors are only caused by server overload, and by nothing else
   */
  val ServerErrorsMeansDroppedResult: HttpResponse => Outcome = r =>
    r.status match {
      case ServerError(_) => Dropped
      case ClientError(_) => Ignored
      case _              => Processed
    }

  def apply(limitAlgorithm: Limit,
            maxLiFoQueueDepth: Int = 32,
            maxDelay: HttpRequest => FiniteDuration = _ => 20.millis,
            response: HttpRequest => HttpResponse = _ => HttpServerConcurrencyLimit.TooManyRequestsResponse,
            result: HttpResponse => Outcome = DefaultResult,
            name: String = "http-server-limiter")(implicit system: ActorSystem): HttpConcurrencyLimitConfig = {
    val config = system.settings.config
    val reqestTimeout = {
      val t = config.getDuration("akka.http.server.request-timeout")
      FiniteDuration(t.toNanos, TimeUnit.NANOSECONDS)
    }
    val pipeliningLimit = config.getInt("akka.http.server.pipelining-limit")
    new HttpConcurrencyLimitConfig(
      limitAlgorithm,
      maxLiFoQueueDepth,
      name,
      pipeliningLimit,
      reqestTimeout,
      maxDelay,
      response,
      result
    )
  }
}
