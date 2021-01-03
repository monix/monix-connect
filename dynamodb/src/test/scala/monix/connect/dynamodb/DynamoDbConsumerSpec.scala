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

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.exceptions.DummyException
import monix.reactive.Observable
import org.mockito.IdiomaticMockito
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import org.mockito.MockitoSugar.{times, verify, when}

import scala.util.{Failure, Success}

class DynamoDbConsumerSpec extends AnyWordSpecLike with Matchers with IdiomaticMockito {

  implicit val client: DynamoDbAsyncClient = mock[DynamoDbAsyncClient]

  s"A ${DynamoDb} Consumer" when {

    s"three operations are passed" must {

      "execute each one exactly once" in {
        //given
        val n = 3
        val req = mock[DynamoDbRequest]
        val resp = mock[DynamoDbResponse]
        val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]

        //when
        when(op.apply(req)(client)).thenReturn(Task(resp))
        Observable
          .fromIterable(List.fill(n)(req))
          .consumeWith(DynamoDb.consumer(retries = 1)(op, client))
          .runSyncUnsafe()

        //when
        verify(op, times(n)).apply(req)
      }

      "supports an empty observable" in {
        //given
        val n = 3
        val req = mock[DynamoDbRequest]
        val resp = mock[DynamoDbResponse]
        val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]

        //when
        when(op.apply(req)(client)).thenReturn(Task(resp))
        val t: Task[Unit] =
          Observable.empty.consumeWith(DynamoDb.consumer(retries = 1)(op, client))

        //when
        t.runToFuture.value shouldBe Some(Success(()))
      }

      "correctly reports error on failure" in {
        //given
        val n = 3
        val req = mock[DynamoDbRequest]
        val resp = mock[DynamoDbResponse]
        val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]
        val ex = DummyException("DynamoDB is busy.")
        when(op.apply(req)(client)).thenReturn(Task(resp), Task.raiseError(ex), Task(resp))

        //when
        val f =
          Observable
            .fromIterable(List.fill(n)(req))
            .consumeWith(DynamoDb.consumer(retries = 0)(op, client))
            .runToFuture

        //then
        verify(op, times(2)).apply(req)
        f.value shouldBe Some(Failure(ex))
      }

      "retries the operation if there were a failure" in {
        //given
        val n = 3
        val req = mock[DynamoDbRequest]
        val resp = mock[DynamoDbResponse]
        val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]
        val ex = DummyException("DynamoDB is busy.")
        when(op.apply(req)(client)).thenReturn(Task(resp), Task.raiseError(ex), Task(resp))

        //when
        val f =
          Observable
            .fromIterable(List.fill(n)(req))
            .consumeWith(DynamoDb.consumer(retries = 1)(op, client))
            .runToFuture

        //then
        verify(op, times(n + 1)).apply(req)
        f.value shouldBe Some(Success(()))
      }

      "reports failure after all retries were exhausted" in {
        //given
        val n = 3
        val req = mock[DynamoDbRequest]
        val resp = mock[DynamoDbResponse]
        val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]
        val ex = DummyException("DynamoDB is busy.")
        when(op.apply(req)(client)).thenReturn(Task(resp), Task.raiseError(ex), Task.raiseError(ex), Task(resp))

        //when
        val f =
          Observable
            .fromIterable(List.fill(n)(req))
            .consumeWith(DynamoDb.consumer(retries = 1)(op, client))
            .runToFuture

        //then
        verify(op, times(3)).apply(req)
        f.value shouldBe Some(Failure(ex))
      }

    }
  }

}
