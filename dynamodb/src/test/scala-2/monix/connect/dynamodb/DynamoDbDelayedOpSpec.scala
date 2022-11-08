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
import monix.execution.schedulers.TestScheduler
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{times, verify, when}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.concurrent.duration.DurationInt
import scala.util.Success

class DynamoDbDelayedOpSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers with Fixture {

  s"$DynamoDbOp" should {
    implicit val s = TestScheduler()
    val retryStrategy = RetryStrategy(retries = 3)
    implicit val client: DynamoDbAsyncClient = mock[DynamoDbAsyncClient]
    val req = mock[DynamoDbRequest]

    "delays a single failed execution by the configured finite duration" in {
      //given
      val delay = 2.seconds
      val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]
      val ex = DummyException("DynamoDB is busy.")
      when(op.apply(req)(client))
        .thenReturn(Task.raiseError(ex), Task(resp))

      //when
      val f = DynamoDb.createUnsafe(client).single(req, retryStrategy.copy(backoffDelay = delay))(op).runToFuture(s)

      //then
      s.tick(1.second)
      verify(op, times(1)).apply(req)

      //and
      s.tick(delay * 2)
      verify(op, times(2)).apply(req)
      f.value shouldBe Some(Success(resp))
    }

    "delays each failed execution by the configured finite duration" in {
      //given
      val delay = 2.seconds
      val op = mock[DynamoDbOp[DynamoDbRequest, DynamoDbResponse]]
      val ex = DummyException("DynamoDB is busy.")
      when(op.apply(req)(client))
        .thenReturn(Task.raiseError(ex), Task.raiseError(ex), Task.raiseError(ex), Task(resp))

      //when
      val f = DynamoDb.createUnsafe(client).single(req, retryStrategy.copy(backoffDelay = delay))(op).runToFuture(s)

      //then
      s.tick(1.second)
      verify(op, times(1)).apply(req)

      //then
      s.tick(2.seconds)
      verify(op, times(2)).apply(req)

      //then
      s.tick(2.seconds)
      verify(op, times(3)).apply(req)

      //and
      s.tick(delay * 2)
      verify(op, times(4)).apply(req)
      f.value shouldBe Some(Success(resp))
    }
  }
}
