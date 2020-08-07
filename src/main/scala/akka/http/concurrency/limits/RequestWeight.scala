package akka.http.concurrency.limits

import akka.http.scaladsl.model.AttributeKey


case class RequestWeight(weight: Int) {
  require(weight > 0, s"Weight must be at least 1, got $weight")
}

object RequestWeight {

  val attributeKey: AttributeKey[RequestWeight] = AttributeKey[RequestWeight]("weight")
}
