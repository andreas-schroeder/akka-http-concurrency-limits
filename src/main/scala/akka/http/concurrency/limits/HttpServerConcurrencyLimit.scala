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
    config: HttpLiFoQueuedConcurrencyLimitConfig
  )(implicit system: ActorSystem): BidiFlow[HttpRequest, HttpRequest, HttpResponse, HttpResponse, NotUsed] = {
    import config._
    val typed = system.toTyped

    val limitActor: ActorRef[LimitActor.LimitActorCommand] =
      typed.systemActorOf(
        LimitActor.liFoQueued(limitAlgorithm, maxLiFoQueueDepth, batchSize, batchTimeout, maxDelay, requestTimeout),
        config.name
      )
    GlobalLimitBidiFlow(limitActor, pipeliningLimit, requestTimeout, weight, rejectionResponse, result)
  }

  val TooManyRequestsResponse: HttpResponse = HttpResponse(StatusCodes.TooManyRequests, entity = "Too many requests")
}

final case class HttpLiFoQueuedConcurrencyLimitConfig(limitAlgorithm: Limit,
                                                      maxLiFoQueueDepth: Int,
                                                      batchSize: Int,
                                                      batchTimeout: FiniteDuration,
                                                      name: String,
                                                      pipeliningLimit: Int,
                                                      requestTimeout: FiniteDuration,
                                                      maxDelay: FiniteDuration,
                                                      weight: HttpResponse => Int,
                                                      rejectionResponse: HttpRequest => HttpResponse,
                                                      result: HttpResponse => Outcome)

object HttpLiFoQueuedConcurrencyLimitConfig {

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

  val requestWeightExtractor: HttpResponse => Int = _.attribute(RequestWeight.attributeKey).map(_.weight).getOrElse(1)

  /**
    *
    * @param limitAlgorithm the limit algorithm to use.
    * @param maxLiFoQueueDepth max queue depth - this is multiplied with the current concurrency limit to determine
    *                          queue length.
    * @param batchSize amount of requests that may be served given a single capacity grant of the global limiter actor.
    * @param batchTimeout validity time of capacity grant batches provided by the global limiter actor.
    * @param maxDelay the maximum time to wait in the lifo queue for available capacity.
    * @param weight processing cost of request to adjust latency measurements.
    * @param rejectionResponse function to compute the response to give when rejecting a request.
    * @param result how to evaluate the response in terms of latency: was the request dropped, was it successfully
    *               processed, or should it be ignored for computing the adaptive concurrency limit
    * @param name name of the limit actor. Must be globally unique. Specify if you need to create more than one
    *             server limiter.
    * @param system the actor system to use. This is used to fetch request timeout and pipelining limits from the akka
    *               http server config.
    */
  def apply(
    limitAlgorithm: Limit,
    maxLiFoQueueDepth: Int = 16,
    batchSize: Int = 10,
    batchTimeout: FiniteDuration = 500.millis,
    maxDelay: FiniteDuration = 50.millis,
    weight: HttpResponse => Int = requestWeightExtractor,
    rejectionResponse: HttpRequest => HttpResponse = _ => HttpServerConcurrencyLimit.TooManyRequestsResponse,
    result: HttpResponse => Outcome = DefaultResult,
    name: String = "http-server-limiter"
  )(implicit system: ActorSystem): HttpLiFoQueuedConcurrencyLimitConfig = {
    val config = system.settings.config
    val requestTimeout = {
      val t = config.getDuration("akka.http.server.request-timeout")
      FiniteDuration(t.toNanos, TimeUnit.NANOSECONDS)
    }
    val pipeliningLimit = config.getInt("akka.http.server.pipelining-limit")
    new HttpLiFoQueuedConcurrencyLimitConfig(
      limitAlgorithm,
      maxLiFoQueueDepth,
      batchSize,
      batchTimeout,
      name,
      pipeliningLimit,
      requestTimeout,
      maxDelay,
      weight,
      rejectionResponse,
      result
    )
  }
}
