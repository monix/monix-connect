/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
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

import monix.catnap.MVar
import monix.eval.Task
import monix.execution.FutureUtils.Java8Extensions
import monix.execution.Scheduler
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse}

import java.util.concurrent.CompletableFuture
import scala.concurrent.Future

trait Fixture {

  val client: DynamoDbAsyncClient = new DynamoDbAsyncClient {
    override def serviceName(): String = ???
    override def close(): Unit = ???
  }

  val req = GetItemRequest.builder().build()
  val resp = GetItemResponse.builder().build()

  def withOperationStub(f: Int => Task[GetItemResponse])(implicit s: Scheduler) =
    new DynamoDbOp[GetItemRequest, GetItemResponse] {
      override def apply(dynamoDbRequest: GetItemRequest)(implicit client: DynamoDbAsyncClient): Task[GetItemResponse] =
        Task.defer(Task.from(execute(dynamoDbRequest)))
      val counterMvarTask: Task[MVar[Task, Int]] = MVar[Task].of(1).memoize
      def incWithF(): Task[GetItemResponse] =
        counterMvarTask.flatMap(counterMvar =>
          counterMvar.take.flatMap(value => counterMvar.put(value + 1) >> f(value)))

      override def execute(dynamoDbRequest: GetItemRequest)(
        implicit client: DynamoDbAsyncClient): CompletableFuture[GetItemResponse] = {
        incWithF.runToFuture.flatMap(r => Future.successful(r)).asJava.toCompletableFuture
      }
    }

}
