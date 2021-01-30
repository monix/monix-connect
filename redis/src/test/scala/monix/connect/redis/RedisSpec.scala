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

package monix.connect.redis

import io.lettuce.core.api.StatefulRedisConnection
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class RedisSpec
  extends AnyWordSpecLike with Matchers with IdiomaticMockito with BeforeAndAfterEach with BeforeAndAfterAll
  with RedisFixture {

  implicit val connection: StatefulRedisConnection[String, Int] = mock[StatefulRedisConnection[String, Int]]

  override def beforeAll(): Unit = {
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    reset(reactiveRedisCommands)
    reset(reactiveRedisCommands)
  }

  /*
  s"${$Commands} " should {
    " implement RedisKey" in {
      $Commands shouldBe a[KeyCommands]
    }
    "implement RedisHash" in {
      $Commands shouldBe a[HashCommands]
    }
    "implement RedisList" in {
      $Commands shouldBe a[ListCommands]
    }
    "implement RedisPubSub" in {
      $Commands shouldBe a[RedisPubSub]
    }
    "implement RedisSet" in {
      $Commands shouldBe a[SetCommands]
    }
    "implement RedisSortedSet" in {
      $Commands shouldBe a[SortedSetCommands]
    }
    "implement RedisStream" in {
      $Commands shouldBe a[RedisStream]
    }
    "implement RedisString" in {
      $Commands shouldBe a[StringCommands]
    }
  }
*/
}
