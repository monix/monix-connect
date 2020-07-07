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

import java.lang.Thread.sleep

import monix.eval.Task
import monix.execution.exceptions.DummyException
import monix.execution.schedulers.TestScheduler
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{times, verify, when}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class DynamoDbOpSpec extends AnyWordSpecLike with Matchers with IdiomaticMockito {

  implicit val client: DynamoDbAsyncClient = mock[DynamoDbAsyncClient]

  implicit val s = TestScheduler()

  s"${DynamoDbOp}" should {

    "create the description of the operation's execution as a recursive function" that {

      val retries = 3
      val req = mock[DynamoDbRequest]
      val resp = mock[DynamoDbResponse]

      "retries on failure as many times as configured" in {
        //given
        val req = mock[DynamoDbRequest]
        val resp = mock[DynamoDbResponse]
        val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]
        val ex = DummyException("DynamoDB is busy.")
        when(op.apply(req)(client))
          .thenReturn(Task.raiseError(ex), Task.raiseError(ex), Task.raiseError(ex), Task(resp))

        //when
        val f = DynamoDbOp.create(req, retries, None)(op, client).runToFuture

        //then
        verify(op, times(retries + 1)).apply(req)
        f.value shouldBe Some(Success(resp))
      }

      "retries failed executions and correctly returns the last error when retries were exhausted" in {
        //given
        val req = mock[DynamoDbRequest]
        val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]
        val ex = DummyException("DynamoDB is busy.")
        val lastEx = DummyException("Final Exception")
        when(op.apply(req)(client))
          .thenReturn(Task.raiseError(ex), Task.raiseError(ex), Task.raiseError(ex), Task.raiseError(lastEx))

        //when
        val f = DynamoDbOp.create(req, retries, None)(op, client).runToFuture

        //then
        verify(op, times(retries + 1)).apply(req)
        f.value shouldBe Some(Failure(lastEx))
      }

      "delays each failed execution by the configured finite duration" in {
        //given
        val delay = 2.seconds
        val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]
        val ex = DummyException("DynamoDB is busy.")
        when(op.apply(req)(client))
          .thenReturn(Task.raiseError(ex), Task(resp))

        //when
        val f = DynamoDbOp.create(req, retries, Some(delay))(op, client).runToFuture

        //then
        verify(op, times(1)).apply(req)

        //and
        s.tick(delay * 2)
        verify(op, times(2)).apply(req)
        f.value shouldBe Some(Success(resp))
      }

    }
  }
}
