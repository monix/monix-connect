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

import io.lettuce.core.{KeyValue, ValueScanCursor}
import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import monix.reactive.Observable
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class RedisSetSpec
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

  s"${RedisSet} " should " implement RedisSet trait" in {
    RedisSet shouldBe a[RedisSet]
  }

  it should "implement sadd" in {
    //given
    val key: String = genRedisKey.sample.get
    val members: List[Int] = genRedisValues.sample.get
    when(asyncRedisCommands.sadd(key, members: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisSet.sadd(key, members: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).sadd(key, members: _*)
  }

  it should "implement scard" in {
    //given
    val key: String = genRedisKey.sample.get
    when(asyncRedisCommands.scard(key)).thenReturn(longRedisFuture)

    //when
    val t = RedisSet.scard(key)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).scard(key)
  }

  it should "implement sdiff" in {
    //given
    val keys: List[String] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sdiff(keys: _*)).thenReturn(MockFlux[Int])

    //when
    val t = RedisSet.sdiff(keys: _*)

    //then
    t shouldBe a[Observable[Int]]
    verify(reactiveRedisCommands).sdiff(keys: _*)
  }

  it should "implement sdiffstore" in {
    //given
    val dest: String = genRedisKey.sample.get
    val keys: List[String] = genRedisKeys.sample.get
    when(asyncRedisCommands.sdiffstore(dest, keys: _*)).thenReturn(longRedisFuture)

    //when
    val t = RedisSet.sdiffstore(dest, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).sdiffstore(dest, keys: _*)
  }

  it should "implement sinter" in {
    //given
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sinter(keys: _*)).thenReturn(MockFlux[Int])

    //when
    val t = Redis.sinter(keys: _*)

    //then
    t shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).sinter(keys: _*)
  }

  it should "implement sinterstore" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.sinterstore(dest, keys: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.sinterstore(dest, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).sinterstore(dest, keys: _*)
  }

  it should "implement sismember" in {
    //given
    val key: String = genRedisKey.sample.get
    val member: Int = genRedisValue.sample.get
    when(asyncRedisCommands.sismember(key, member)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = Redis.sismember(key, member)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).sismember(key, member)
  }

  it should "implement smove" in {
    //given
    val source: String = genRedisKey.sample.get
    val dest: String = genRedisKey.sample.get
    val member: Int = genRedisValue.sample.get
    when(asyncRedisCommands.smove(source, dest, member)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = Redis.smove(source, dest, member)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).smove(source, dest, member)
  }

  it should "implement smembers" in {
    //given
    val key: String = genRedisKey.sample.get
    when(reactiveRedisCommands.smembers(key)).thenReturn(MockFlux[V])

    //when
    val t = Redis.smembers(key)

    //then
    t shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).smembers(key)
  }

  it should "implement spop" in {
    //given
    val key: String = genRedisKey.sample.get
    val count: Long = genLong.sample.get
    when(asyncRedisCommands.spop(key)).thenReturn(MockRedisFuture[V])
    when(reactiveRedisCommands.spop(key, count)).thenReturn(MockFlux[V])

    //when
    val r1 = Redis.spop(key)
    val r2 = RedisSet.spop(key, count)

    //then
    r1 shouldBe a[Task[V]]
    r2 shouldBe a[Observable[V]]
    verify(asyncRedisCommands).spop(key)
    verify(reactiveRedisCommands).spop(key, count)
  }

  it should "implement srandmember" in {
    //given
    val key: String = genRedisKey.sample.get
    val count: Long = genLong.sample.get
    when(asyncRedisCommands.srandmember(key)).thenReturn(MockRedisFuture[V])
    when(reactiveRedisCommands.srandmember(key, count)).thenReturn(MockFlux[V])

    //when
    val r1 = Redis.srandmember(key)
    val r2 = Redis.srandmember(key, count)

    //then
    r1 shouldBe a[Task[V]]
    r2 shouldBe a[Observable[V]]
    verify(asyncRedisCommands).srandmember(key)
    verify(reactiveRedisCommands).srandmember(key, count)
  }

  it should "implement srem" in {
    //given
    val key: String = genRedisKey.sample.get
    val members: List[V] = genRedisValues.sample.get
    when(asyncRedisCommands.srem(key, members: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val r = RedisSet.srem(key, members: _*)

    //then
    r shouldBe a[Task[Long]]
    verify(asyncRedisCommands).srem(key, members: _*)
  }

  it should "implement sunion" in {
    //given
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sunion(keys: _*)).thenReturn(MockFlux[V])

    //when
    val r = RedisSet.sunion(keys: _*)

    //then
    r shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).sunion(keys: _*)
  }

  it should "implement sunionstore" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.sunionstore(dest, keys: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val r = RedisSet.sunionstore(dest, keys: _*)

    //then
    r shouldBe a[Task[Long]]
    verify(asyncRedisCommands).sunionstore(dest, keys: _*)
  }

  it should "implement sscan" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.sscan(key)).thenReturn(MockRedisFuture[ValueScanCursor[V]])
    //wiht scanArgs and scanCursor not supported

    //when
    val r = RedisSet.sscan(key)

    //then
    r shouldBe a[Task[ValueScanCursor[V]]]
    verify(asyncRedisCommands).sscan(key)
  }

}
