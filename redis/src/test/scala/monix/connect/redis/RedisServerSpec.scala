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
import monix.eval.Task
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class RedisServerSpec
  extends AnyFlatSpec with Matchers with IdiomaticMockito with BeforeAndAfterEach with BeforeAndAfterAll
    with RedisFixture {

  implicit val connection: StatefulRedisConnection[String, Int] = mock[StatefulRedisConnection[String, Int]]

  override def beforeAll(): Unit = {
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    super.beforeAll()
  }

  override def beforeEach() = {
    reset(reactiveRedisCommands)
    reset(reactiveRedisCommands)
  }

  s"${RedisServer}" should "extend the RedisServer trait" in {
    RedisServer shouldBe a[RedisServer]
  }

  it should "implement flushall operation" in {
    //given
    when(reactiveRedisCommands.flushallAsync()).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisServer.flushallAsync()

    //then
    verify(reactiveRedisCommands).flushallAsync()
  }

  it should "implement flushdb operation" in {
    //given
    when(reactiveRedisCommands.flushdbAsync()).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisServer.flushdbAsync()

    //then
    verify(reactiveRedisCommands).flushdbAsync()
  }

}
