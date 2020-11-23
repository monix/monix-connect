/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.dynamodb

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

package object domain {

  @deprecated("See `RetryStrategy`")
  case class RetrySettings(retries: Int, delayAfterFailure: Option[FiniteDuration])
  final val DefaultRetrySettings = RetrySettings(0, Option.empty)

  case class RetryStrategy(retries: Int, backoffDelay: FiniteDuration)
  final val DefaultRetryStrategy = RetryStrategy(0, 0.seconds)

}
