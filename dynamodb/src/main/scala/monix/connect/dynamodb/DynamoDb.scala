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

import monix.reactive.{Consumer, Observable}
import monix.eval.Task
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

import scala.concurrent.duration.FiniteDuration

/**
  * An idiomatic DynamoDb client integrated with Monix ecosystem.
  *
  * It is built on top of the [[DynamoDbAsyncClient]], reason why all the exposed methods
  * expect an implicit instance of the client to be in the scope of the call.
  */
object DynamoDb {

  /**
    * Pre-built [[Consumer]] implementation that expects and executes [[DynamoDbRequest]]s.
    * It provides with the flexibility of retrying a failed execution with delay to recover from it.
    *
    * @param dynamoDbOp abstracts the execution of any given [[DynamoDbRequest]] with its correspondent operation that returns [[DynamoDbResponse]].
    * @param client an asyncronous dynamodb client.
    * @tparam In the input request as type parameter lower bounded by [[DynamoDbRequest]].
    * @tparam Out output type parameter that must be a subtype os [[DynamoDbRequest]].
    * @return A [[monix.reactive.Consumer]] that expects and executes dynamodb requests.
    */
  def consumer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    retries: Int = 0,
    delayAfterFailure: Option[FiniteDuration] = None)(
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient): Consumer[In, Unit] = DynamoDbSubscriber(retries, delayAfterFailure)

  /**
    * Transformer that executes any given [[DynamoDbRequest]] and transforms them to its subsequent [[DynamoDbResponse]] within [[Task]].
    * It also provides with the flexibility of retrying a failed execution with delay to recover from it.
    *
    * @param retries the number of times that an operation can be retried before actually returning a failed [[Task]].
    *        it must be higher or equal than 0.
    * @param delayAfterFailure delay after failure for the execution of a single [[DynamoDbOp]].
    * @param dynamoDbOp implicit [[DynamoDbOp]] that abstracts the execution of the specific operation.
    * @param client asynchronous DynamoDb client.
    * @tparam In input type parameter that must be a subtype os [[DynamoDbRequest]].
    * @tparam Out output type parameter that must be a subtype os [[DynamoDbRequest]].
    * @return DynamoDb operation transformer: `Observable[DynamoDbRequest] => Observable[DynamoDbRequest]`.
    */
  def transformer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    retries: Int = 0,
    delayAfterFailure: Option[FiniteDuration] = None)(
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient): Observable[In] => Observable[Task[Out]] = { inObservable: Observable[In] =>
    inObservable.map(request => DynamoDbOp.create(request, retries, delayAfterFailure))
  }

}
