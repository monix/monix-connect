/*
 * Copyright (c) 2014-2020 by The Monix Connect Project Developers.
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

import monix.connect.common.Operators.Transformer
import monix.reactive.{Consumer, Observable, Observer}
import monix.execution.Ack
import monix.eval.Task
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

import scala.jdk.FutureConverters._

object DynamoDb {

  def consumer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient = DynamoDbClient()): Consumer[In, Task[Out]] = {
    Consumer.create[In, Task[Out]] { (_, _, callback) =>
      new Observer.Sync[In] {
        private var dynamoDbResponse: Task[Out] = _

        def onNext(dynamoDbRequest: In): Ack = {
          dynamoDbResponse = Task.fromFuture(dynamoDbOp.execute(dynamoDbRequest).asScala)
          monix.execution.Ack.Continue
        }

        def onComplete(): Unit = {
          callback.onSuccess(dynamoDbResponse)
        }

        def onError(ex: Throwable): Unit = {
          callback.onError(ex)
        }
      }
    }
  }

  def transformer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient = DynamoDbClient()): Transformer[In, Task[Out]] = { inObservable: Observable[In] =>
    inObservable.map(in => Task.fromFuture(dynamoDbOp.execute(in).asScala))
  }
}
