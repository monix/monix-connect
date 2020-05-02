/*
 * Copyright (c) 2014-2020 by The Monix Connect Project Developers.
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

import io.lettuce.core.KeyValue
import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import monix.reactive.Observable
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

class RedisListSpec
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
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
  }

  s"${RedisList} " should " implement RedisList trait" in {
    RedisList shouldBe a[RedisList]
  }

  it should "implement blpop" in {
    //given
    val timeout: Long = genLong.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.blpop(timeout, keys: _*)).thenReturn(KVRedisFuture)

    //when
    val t = Redis.blpop(timeout, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).blpop(timeout, keys: _*)
  }

  it should "implement brpop" in {
    //given
    val timeout: Long = genLong.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.brpop(timeout, keys: _*)).thenReturn(KVRedisFuture)

    //when
    val t = Redis.brpop(timeout, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).brpop(timeout, keys: _*)
  }

  it should "implement brpoplpush" in {
    //given
    val timeout: Long = genLong.sample.get
    val source: K = genRedisKey.sample.get
    val dest: K = genRedisKey.sample.get
    when(asyncRedisCommands.brpoplpush(timeout, source, dest)).thenReturn(intRedisFuture)

    //when
    val t = Redis.brpoplpush(timeout, source, dest)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).brpoplpush(timeout, source, dest)
  }

  it should "implement lindex" in {
    //given
    val index: Long = genLong.sample.get
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.lindex(key, index)).thenReturn(vRedisFuture)

    //when
    val t = Redis.lindex(key, index)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).lindex(key, index)
  }

  it should "implement linsert" in {
    //given
    val index: Long = genLong.sample.get
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.lindex(key, index)).thenReturn(vRedisFuture)

    //when
    val t = Redis.lindex(key, index)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).lindex(key, index)
  }

  it should "implement llen" in {
    //given
    val key: String = genRedisKey.sample.get
    when(asyncRedisCommands.llen(key)).thenReturn(longRedisFuture)

    //when
    val t = Redis.llen(key)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).llen(key)
  }

  it should "implement lpop" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.lpop(key)).thenReturn(MockRedisFuture[Int])

    //when
    val t = Redis.lpop(key)

    //then
    t shouldBe a[Task[Int]]
    verify(asyncRedisCommands).lpop(key)
  }

  it should "implement lpush" in {
    //given
    val key: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get
    when(asyncRedisCommands.lpush(key, values: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.lpush(key, values: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).lpush(key, values: _*)
  }

  it should "implement lpushx" in {
    //given
    val key: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get
    when(asyncRedisCommands.lpushx(key, values: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.lpushx(key, values: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).lpushx(key, values: _*)
  }

  it should "implement lrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop = genLong.sample.get
    when(reactiveRedisCommands.lrange(key, start, stop)).thenReturn(MockFlux[Int])

    //when
    val t = Redis.lrange(key, start, stop)

    //then
    t shouldBe a[Observable[Int]]
    verify(reactiveRedisCommands).lrange(key, start, stop)
  }

  it should "implement lrem" in {
    //given
    val key: String = genRedisKey.sample.get
    val count: Long = genLong.sample.get
    val value: Int = genRedisValue.sample.get
    when(asyncRedisCommands.lrem(key, count, value)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.lrem(key, count, value)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).lrem(key, count, value)
  }

  it should "implement lset" in {
    //given
    val key: K = genRedisKey.sample.get
    val index: Long = genLong.sample.get
    val value: V = genRedisValue.sample.get
    when(asyncRedisCommands.lset(key, index, value)).thenReturn(MockRedisFuture[String])

    //when
    val t = Redis.lset(key, index, value)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).lset(key, index, value)
  }

  it should "implement ltrim" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(asyncRedisCommands.ltrim(key, start, stop)).thenReturn(MockRedisFuture[String])

    //when
    val t = Redis.ltrim(key, start, stop)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).ltrim(key, start, stop)
  }

  it should "implement rpop" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.rpop(key)).thenReturn(MockRedisFuture[Int])

    //when
    val t = Redis.rpop(key)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).rpop(key)
  }

  it should "implement rpoplpush" in {
    //given
    val source: K = genRedisKey.sample.get
    val dest: K = genRedisKey.sample.get
    when(asyncRedisCommands.rpoplpush(source, dest)).thenReturn(MockRedisFuture[Int])

    //when
    val t = Redis.rpoplpush(source, dest)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).rpoplpush(source, dest)
  }

  it should "implement rpush" in {
    //given
    val key: String = genRedisKey.sample.get
    val values: List[Int] = genRedisValues.sample.get
    when(asyncRedisCommands.rpush(key, values: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.rpush(key, values: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).rpush(key, values: _*)
  }

  it should "implement rpushx" in {
    //given
    val key: String = genRedisKey.sample.get
    val values: List[Int] = genRedisValues.sample.get
    when(asyncRedisCommands.rpushx(key, values: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.rpushx(key, values: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).rpushx(key, values: _*)
  }

}
