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
import monix.execution.exceptions.DummyException
import monix.execution.Scheduler.Implicits.global
import org.mockito.IdiomaticMockito
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike


class DynamoDbOpSpec extends AnyWordSpecLike with Matchers with IdiomaticMockito with Fixture {

  s"$DynamoDbOp" should {

    "create the description of the operation's execution as a recursive function" that {

      val retryStrategy = RetryStrategy(retries = 3)

      "retries on failure as many times as configured" in {
        //given
        val ex = DummyException("DynamoDB is busy.")
        val op = withOperationStub(i => if (i <= 2) Task.raiseError(ex) else Task.pure(resp))

        //when
        val t = DynamoDb.createUnsafe(client).single(req, retryStrategy)(op)

        //then
        t.attempt.runSyncUnsafe()  shouldBe Right(resp)
      }

      "retries failed executions and correctly returns the last error when retries were exhausted" in {
        //given
        val ex = DummyException("DynamoDB is busy.")
        val lastEx = DummyException("Final Exception")
        val op = withOperationStub(i => if (i < 2) Task.raiseError(ex) else Task.raiseError(lastEx))

        //when
        val t = DynamoDb.createUnsafe(client).single(req, retryStrategy)(op)

        //then
        t.attempt.runSyncUnsafe() shouldBe Left(lastEx)
      }
    }
  }
}
