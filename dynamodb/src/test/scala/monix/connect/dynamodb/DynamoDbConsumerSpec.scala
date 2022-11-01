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

import monix.connect.dynamodb.domain.RetryStrategy
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.exceptions.DummyException
import monix.reactive.Observable
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.model._

import scala.util.Success

class DynamoDbConsumerSpec extends AnyWordSpecLike with Matchers with Fixture {


  s"A $DynamoDb Consumer" when {

    s"three operations are passed" must {

      "execute each one exactly once" in {

        //given
        val n = 3

        val op = withOperationStub(_ => Task.pure(resp))

        //when/then
        Observable
          .fromIterable(List.fill(n)(req))
          .consumeWith(DynamoDb.createUnsafe(client).sink()(op))
          .runSyncUnsafe()
      }

      "supports an empty observable" in {
        //given
        val op = withOperationStub(_ => Task.pure(resp))

        //when
        val t: Task[Unit] =
          Observable.empty.consumeWith(DynamoDb.createUnsafe(client).sink()(op))

        //when
        t.runToFuture.value shouldBe Some(Success(()))
      }

      "correctly reports error on failure" in {
        //given
        val n = 3
        val req = GetItemRequest.builder().build()
        val ex = DummyException("DynamoDB is busy.")
        val op = withOperationStub(_ => Task.raiseError(ex))

        //when
        val f =
          Observable
            .fromIterable(List.fill(n)(req))
            .consumeWith(DynamoDb.createUnsafe(client).sink(RetryStrategy(retries = 0))(op))

        //then
        f.attempt.runSyncUnsafe() shouldBe Left(ex)
      }


      "retries the operation if there was a failure" in {

        //given
        val n = 3
        val ex = DummyException("DynamoDB is busy.")
        val op = withOperationStub(i => if(i<2) Task.raiseError(ex) else Task.pure(resp))

        //when
        val t =
          Observable
            .fromIterable(List.fill(n)(req))
            .consumeWith(DynamoDb.createUnsafe(client).sink(RetryStrategy(retries = 1))(op))

        //then
        t.attempt.runSyncUnsafe() shouldBe Right(())
      }

      "reports failure after all retries were exhausted" in {
        //given
        val n = 3

        val ex = DummyException("DynamoDB is busy.")
        val op = withOperationStub(i => if(i<4) Task.raiseError(ex) else Task.pure(resp))

        //when
        val t =
          Observable
            .fromIterable(List.fill(n)(req))
            .consumeWith(DynamoDb.createUnsafe(client).sink(RetryStrategy(retries = 1))(op))

        //then
        t.attempt.runSyncUnsafe() shouldBe Left(ex)
      }

    }
  }

}
