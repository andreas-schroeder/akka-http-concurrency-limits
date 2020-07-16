# Akka Http Concurrency Limits

[Akka http](https://github.com/akka/akka-http) implementation for Netflix's [adaptive concurrency limits](https://github.com/Netflix/concurrency-limits).

In good reactive manner, this adapter avoids blocking threads and synchronization by
using akka stream primitives and actors.

## Installation

## Usage

Apply an (adaptive) concurrency limit to an http route passed as follows:

```scala
import akka.actor.ActorSystem
import akka.http.concurrency.limits._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import com.netflix.concurrency.limits.limit.FixedLimit


implicit val system: ActorSystem = ActorSystem()

val config = HttpLiFoQueuedConcurrencyLimitConfig(FixedLimit.of(256))
val limitFlow = HttpServerConcurrencyLimit.liFoQueued(config)

val route = pathSingleSlash {
    get {
      complete("Ok")
    }
  }

Http().bindAndHandle(limitFlow join route, "0.0.0.0", 8080)
```

In the above example, `limitFlow` is a bidirectional flow that wraps the route
and ensures that the number of in-flight http requests do never exceed the current concurrency
limit. The `config` object allows to modify the behavior of the global concurrency limit.

## LiFoQueued Configuration

| Config            | Description | Default |
| ------------------|-------------|---------|
| limitAlgorithm    | the limit algorithm to use. | none |
| maxLiFoQueueDepth | max queue depth - this is multiplied with the current concurrency limit to determine queue length. | 16 |
| maxDelay          | the maximum time to wait in the lifo queue for available capacity. | 50 ms |
| rejectionResponse | function to compute the response to give when rejecting a request. | Http 429 - too many requests |
| result            | how to evaluate the response in terms of latency: was the request dropped, was it successfully processed, or should it be ignored for computing the adaptive concurrency limit. | ignore client and server errors, measure all others |
| name              | name of the limit actor. Must be globally unique. Specify if you need to create more than one server limiter. | http-server-limiter |

## Available Limiters

The Netflix-provided concurrency limit algorithms need to be managed by limiters providing additional capabilities.
Currently, Akka http concurrency limits offers the following limiters.   

### LiFoQueued Limiter

A limiter that features a LiFo-queue (last in, first out). Once the current concurrency limit is reached, this limiter
starts delaying requests for up to a configurable amount of time (see the config object). The requests are dequeued
in reverse order in order to ensure higher success rate under overload, while disregarding fairness towards clients.

## Using Limiters in other contexts

This library provides three building blocks that can be used in other contexts, especially around `mapAsync` stream 
stages performing external I/O. The `HttpServerConcurrencyLimit.liFoQueued` is made up of a `LiFoQueuedLimitActor` 
managing the concurrency limit, and a `GlobaLimitBidiFlow` whose materializations interact with a given limit actor to
ask for capacity. These two can be used to globally limit the concurrency of multiple independent `mapAsync` stream stages.
The third building block is the `LocalLimitBidiFlow` that manages the limit algorithm directly. 

```scala
import akka.actor.ActorSystem
import akka.http.concurrency.limits._
import akka.http.concurrency.limits.LimitBidiFolow._
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.netflix.concurrency.limits.limit.FixedLimit
import scala.concurrent.Future
import scala.concurrent.duration._

implicit val sys = ActorSystem()
implicit val mat = Materializer(sys)
implicit val ec = sys.dispatcher

val maxParallelism = 20

val limitFlow =
  LocalLimitBidiFlow[Int, String](() => FixedLimit.of(10), maxParallelism, _ => "Rejected", _ => Processed)

val mapAsync = Flow[Int].mapAsyncUnordered(maxParallelism) { _ =>
  akka.pattern.after(10.millis, sys.scheduler)(Future.successful("Accepted"))
}

Source.repeat(1).take(20).via(limitFlow join mapAsync).runWith(Sink.collection)
```

Note that the `LocalLimitBidiFlow` doesn't implement any queuing or delay of messages once the concurrency limit is 
reached. Instead, it immediately rejects elements once at capacity, and by this:
1) defeats any back-pressuring mechanism preceding it. The main use case for concurrency limits is to immediately
   reject when waiting for capacity is not useful, since the response wouldn't be valuable if it isn't immediate.
2) reorders elements in that rejected elements overtake slower accepted ones in the wrapped flow. Use this only when 
   element order does not matter.

## Performance Penalty

Introducing a concurrency limit means introducing a global lock over all route instances that are
otherwise connection-local only. The performance penalty of this is roughly 10% - 15% in both CPU
and throughput for the route above serving only Http 200 Ok. For a route that has a 5 ms delay,
the throughput penalty vanishes, while CPU is still roughly 10% - 15%. Run a benchmark to 
evaluate how much of a penalty it would be for you.