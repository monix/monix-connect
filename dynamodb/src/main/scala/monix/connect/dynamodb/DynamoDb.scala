/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
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

import monix.reactive.{Consumer, Observable, Observer}
import monix.execution.{Ack, Scheduler}
import monix.eval.Task
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

import scala.concurrent.Future
import scala.jdk.FutureConverters._

object DynamoDb {

  /**
    * A monix [[Consumer]] that executes any given [[software.amazon.awssdk.services.dynamodb.model.DynamoDbRequest]].
    * @param dynamoDbOp
    * @param client
    * @tparam In
    * @tparam Out
    * @return
    */
  def consumer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient,
    scheduler: Scheduler): Consumer[In, Out] = new DynamoDbSubscriber()

  /**
    *
    * @param dynamoDbOp
    * @param client
    * @tparam In
    * @tparam Out
    * @return
    */
  def transformer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient): Observable[In] => Observable[Task[Out]] = { inObservable: Observable[In] =>
    inObservable.map(in => Task.fromFuture(dynamoDbOp.execute(in).asScala))
  }
}
