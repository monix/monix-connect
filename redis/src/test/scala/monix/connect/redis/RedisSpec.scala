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

  s"${Redis} " should {
    " implement RedisKey" in {
      Redis shouldBe a[RedisKey]
    }
    "implement RedisHash" in {
      Redis shouldBe a[RedisHash]
    }
    "implement RedisList" in {
      Redis shouldBe a[RedisList]
    }
    "implement RedisPubSub" in {
      Redis shouldBe a[RedisPubSub]
    }
    "implement RedisSet" in {
      Redis shouldBe a[RedisSet]
    }
    "implement RedisSortedSet" in {
      Redis shouldBe a[RedisSortedSet]
    }
    "implement RedisStream" in {
      Redis shouldBe a[RedisStream]
    }
    "implement RedisString" in {
      Redis shouldBe a[RedisString]
    }
  }

}
