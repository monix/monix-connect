package monix.connect.dynamodb

import scala.concurrent.duration.FiniteDuration

package object domain {

  case class RetrySettings(retries: Int, delayAfterFailure: Option[FiniteDuration])

  val DefaultRetrySettings = RetrySettings(0, Option.empty)
}
