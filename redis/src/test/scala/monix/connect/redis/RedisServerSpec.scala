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

package monix.connect.redis

import java.time.Instant
import java.util.Date

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.{KeyScanCursor, ScanCursor}
import monix.eval.Task
import monix.reactive.Observable
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class RedisServerSpec
  extends AnyFlatSpec with Matchers with IdiomaticMockito with BeforeAndAfterEach with BeforeAndAfterAll
  with RedisFixture {

  implicit val connection: StatefulRedisConnection[String, Int] = mock[StatefulRedisConnection[String, Int]]

  override def beforeAll(): Unit = {
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    super.beforeAll()
  }

  override def beforeEach() = {
    reset(asyncRedisCommands)
    reset(reactiveRedisCommands)
  }

  s"${RedisServer}" should "extend the RedisServer trait" in {
    RedisServer shouldBe a[RedisServer]
  }

  it should "implement flushall operation" in {
    //given
    when(asyncRedisCommands.flushallAsync()).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisServer.flushall()

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).flushallAsync()
  }

  it should "implement flushdb operation" in {
    //given
    when(asyncRedisCommands.flushdbAsync()).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisServer.flushdb()

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).flushdbAsync()
  }

}
