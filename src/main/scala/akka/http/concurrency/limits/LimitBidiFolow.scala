package akka.http.concurrency.limits

object LimitBidiFolow {

  sealed trait Outcome
  case object Processed extends Outcome
  case object Ignored extends Outcome
  case object Dropped extends Outcome
}
