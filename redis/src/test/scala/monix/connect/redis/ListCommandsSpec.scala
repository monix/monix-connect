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

import io.lettuce.core.KeyValue
import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import monix.reactive.Observable
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class ListCommandsSpec
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
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
  }

  /*
  s"${RedisList} " should " implement RedisList trait" in {
    RedisList shouldBe a[ListCommands]
  }

  it should "implement blpop" in {
    //given
    val timeout: Long = genLong.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.blpop(timeout, keys: _*)).thenReturn(mockMono[KeyValue[String, Int]])

    //when
    val _: Task[KeyValue[K, V]] = $Commands.blpop(timeout, keys: _*)

    //then
    verify(reactiveRedisCommands).blpop(timeout, keys: _*)
  }

  it should "implement brpop" in {
    //given
    val timeout: Long = genLong.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.brpop(timeout, keys: _*)).thenReturn(mockMono[KeyValue[String, Int]])

    //when
    val _: Task[KeyValue[K, V]] = $Commands.brpop(timeout, keys: _*)

    //then
    verify(reactiveRedisCommands).brpop(timeout, keys: _*)
  }

  it should "implement brpoplpush" in {
    //given
    val timeout: Long = genLong.sample.get
    val source: K = genRedisKey.sample.get
    val dest: K = genRedisKey.sample.get
    when(reactiveRedisCommands.brpoplpush(timeout, source, dest)).thenReturn(mockMono[Int])

    //when
    val _: Task[V] = $Commands.brpoplpush(timeout, source, dest)

    //then
    verify(reactiveRedisCommands).brpoplpush(timeout, source, dest)
  }

  it should "implement lindex" in {
    //given
    val index: Long = genLong.sample.get
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.lindex(key, index)).thenReturn(mockMono[V])

    //when
    val _: Task[V] = $Commands.lindex(key, index)

    //then
    verify(reactiveRedisCommands).lindex(key, index)
  }

  it should "implement linsert" in {
    //given
    val index: Long = genLong.sample.get
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.lindex(key, index)).thenReturn(mockMono[V])

    //when
    val _: Task[V] = $Commands.lindex(key, index)

    //then
    verify(reactiveRedisCommands).lindex(key, index)
  }

  it should "implement llen" in {
    //given
    val key: String = genRedisKey.sample.get
    when(reactiveRedisCommands.llen(key)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = $Commands.llen(key)

    //then
    verify(reactiveRedisCommands).llen(key)
  }

  it should "implement lpop" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.lpop(key)).thenReturn(mockMono[Int])

    //when
    val _: Task[V] = $Commands.lpop(key)

    //then
    verify(reactiveRedisCommands).lpop(key)
  }

  it should "implement lpush" in {
    //given
    val key: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get
    when(reactiveRedisCommands.lpush(key, values: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = $Commands.lpush(key, values: _*)

    //then
    verify(reactiveRedisCommands).lpush(key, values: _*)
  }

  it should "implement lpushx" in {
    //given
    val key: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get
    when(reactiveRedisCommands.lpushx(key, values: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = $Commands.lpushx(key, values: _*)

    //then
    verify(reactiveRedisCommands).lpushx(key, values: _*)
  }

  it should "implement lrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop = genLong.sample.get
    when(reactiveRedisCommands.lrange(key, start, stop)).thenReturn(mockFlux[Int])

    //when
    val _: Observable[V] = $Commands.lrange(key, start, stop)

    //then
    verify(reactiveRedisCommands).lrange(key, start, stop)
  }

  it should "implement lrem" in {
    //given
    val key: String = genRedisKey.sample.get
    val count: Long = genLong.sample.get
    val value: Int = genRedisValue.sample.get
    when(reactiveRedisCommands.lrem(key, count, value)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = $Commands.lrem(key, count, value)

    //then
    verify(reactiveRedisCommands).lrem(key, count, value)
  }

  it should "implement lset" in {
    //given
    val key: K = genRedisKey.sample.get
    val index: Long = genLong.sample.get
    val value: V = genRedisValue.sample.get
    when(reactiveRedisCommands.lset(key, index, value)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = $Commands.lset(key, index, value)

    //then
    verify(reactiveRedisCommands).lset(key, index, value)
  }

  it should "implement ltrim" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    when(reactiveRedisCommands.ltrim(key, start, stop)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = $Commands.ltrim(key, start, stop)

    //then
    verify(reactiveRedisCommands).ltrim(key, start, stop)
  }

  it should "implement rpop" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.rpop(key)).thenReturn(mockMono[Int])

    //when
    val _: Task[V] = $Commands.rpop(key)

    //then
    verify(reactiveRedisCommands).rpop(key)
  }

  it should "implement rpoplpush" in {
    //given
    val source: K = genRedisKey.sample.get
    val dest: K = genRedisKey.sample.get
    when(reactiveRedisCommands.rpoplpush(source, dest)).thenReturn(mockMono[Int])

    //when
    val _: Task[V] = $Commands.rpoplpush(source, dest)

    //then
    verify(reactiveRedisCommands).rpoplpush(source, dest)
  }

  it should "implement rpush" in {
    //given
    val key: String = genRedisKey.sample.get
    val values: List[Int] = genRedisValues.sample.get
    when(reactiveRedisCommands.rpush(key, values: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = $Commands.rpush(key, values: _*)

    //then
    verify(reactiveRedisCommands).rpush(key, values: _*)
  }

  it should "implement rpushx" in {
    //given
    val key: String = genRedisKey.sample.get
    val values: List[Int] = genRedisValues.sample.get
    when(reactiveRedisCommands.rpushx(key, values: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = $Commands.rpushx(key, values: _*)

    //then
    verify(reactiveRedisCommands).rpushx(key, values: _*)
  }
 */
}
