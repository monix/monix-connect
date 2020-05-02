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

class RedisStringSpec
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

  s"${RedisString} " should " implement RedisString trait" in {
    RedisString shouldBe a[RedisString]
  }

  it should "implement append operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val v: V = genRedisValue.sample.get
    when(asyncRedisCommands.append(k, v)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val r = Redis.append(k, v)

    //then
    r shouldBe a[Task[Long]]
    verify(asyncRedisCommands).append(k, v)
  }

  it should "implement bitcount" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(asyncRedisCommands.bitcount(key)).thenReturn(MockRedisFuture[java.lang.Long])
    when(asyncRedisCommands.bitcount(key, start, stop)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val r1 = Redis.bitcount(key)
    val r2 = Redis.bitcount(key, start, stop)

    //then
    r1 shouldBe a[Task[Long]]
    r2 shouldBe a[Task[Long]]
    verify(asyncRedisCommands).bitcount(key)
    verify(asyncRedisCommands).bitcount(key, start, stop)
  }

  it should "implement bitpos" in {
    //given
    val key: K = genRedisKey.sample.get
    val state: Boolean = genBool.sample.get
    val start: Long = genLong.sample.get
    val end: Long = genLong.sample.get
    when(asyncRedisCommands.bitpos(key, state)).thenReturn(MockRedisFuture[java.lang.Long])
    when(asyncRedisCommands.bitpos(key, state, start)).thenReturn(MockRedisFuture[java.lang.Long])
    when(asyncRedisCommands.bitpos(key, state, start, end)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val r1 = Redis.bitpos(key, state)
    val r2 = Redis.bitpos(key, state, start)
    val r3 = Redis.bitpos(key, state, start, end)

    //then
    r1 shouldBe a[Task[Long]]
    r2 shouldBe a[Task[Long]]
    r3 shouldBe a[Task[Long]]
    verify(asyncRedisCommands).bitpos(key, state)
    verify(asyncRedisCommands).bitpos(key, state, start)
    verify(asyncRedisCommands).bitpos(key, state, start, end)
  }

  it should "implement bitopAnd" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.bitopAnd(dest, keys: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.bitopAnd(dest, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).bitopAnd(dest, keys: _*)
  }

  it should "implement bitopNot" in {
    //given
    val dest: K = genRedisKey.sample.get
    val source: K = genRedisKey.sample.get
    when(asyncRedisCommands.bitopNot(dest, source)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.bitopNot(dest, source)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).bitopNot(dest, source)
  }

  it should "implement bitopOr" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.bitopOr(dest, keys: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = Redis.bitopOr(dest, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).bitopOr(dest, keys: _*)
  }

  it should "implement bitopXor" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.bitopXor(dest, keys: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.bitopXor(dest, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).bitopXor(dest, keys: _*)
  }

  it should "implement decr" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.decr(key)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.decr(key)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).decr(key)
  }

  it should "implement decrby" in {
    //given
    val key: K = genRedisKey.sample.get
    val amount: Long = genLong.sample.get
    when(asyncRedisCommands.decrby(key, amount)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.decrby(key, amount)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).decrby(key, amount)
  }

  it should "implement get" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.get(key)).thenReturn(MockRedisFuture[Int])

    //when
    val t = RedisString.get(key)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).get(key)
  }

  it should "implement getbit" in {
    //given
    val key: K = genRedisKey.sample.get
    val offset: Long = genLong.sample.get
    when(asyncRedisCommands.getbit(key, offset)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.getbit(key, offset)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).getbit(key, offset)
  }

  it should "implement getrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val end: Long = genLong.sample.get
    when(asyncRedisCommands.getrange(key, start, end)).thenReturn(MockRedisFuture[V])

    //when
    val t = RedisString.getrange(key, start, end)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).getrange(key, start, end)
  }

  it should "implement getset" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: V = genRedisValue.sample.get
    when(asyncRedisCommands.getset(key, value)).thenReturn(MockRedisFuture[V])

    //when
    val t = RedisString.getset(key, value)

    //then
    t shouldBe a[Task[V]]
    verify(asyncRedisCommands).getset(key, value)
  }

  it should "implement incr" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.incr(key)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.incr(key)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).incr(key)
  }

  it should "implement incrby" in {
    //given
    val key: K = genRedisKey.sample.get
    val amount: Long = genLong.sample.get
    when(asyncRedisCommands.incrby(key, amount)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.incrby(key, amount)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).incrby(key, amount)
  }

  it should "implement incrbyfloat" in {
    //given
    val key: K = genRedisKey.sample.get
    val amount: Double = genLong.sample.get.toDouble
    when(asyncRedisCommands.incrbyfloat(key, amount)).thenReturn(MockRedisFuture[java.lang.Double])

    //when
    val t = RedisString.incrbyfloat(key, amount)

    //then
    t shouldBe a[Task[Double]]
    verify(asyncRedisCommands).incrbyfloat(key, amount)
  }

  it should "implement mget" in {
    //given
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.mget(keys: _*)).thenReturn(MockFlux[KeyValue[K, V]])

    //when
    val t = RedisString.mget(keys: _*)

    //then
    t shouldBe a[Observable[KeyValue[K, V]]]
    verify(reactiveRedisCommands).mget(keys: _*)
  }

  it should "implement mset" in {
    //given
    val map: Map[K, V] = genKvMap.sample.get
    when(asyncRedisCommands.mset(map.asJava)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisString.mset(map)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).mset(map.asJava)
  }

  it should "implement msetnx" in {
    //given
    val map: Map[K, V] = genKvMap.sample.get
    when(asyncRedisCommands.msetnx(map.asJava)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisString.msetnx(map)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).msetnx(map.asJava)
  }

  it should "implement set" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: V = genRedisValue.sample.get
    when(asyncRedisCommands.set(key, value)).thenReturn(MockRedisFuture[String])
    //with setArgs not supported

    //when
    val t = RedisString.set(key, value)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).set(key, value)
  }

  it should "implement setbit" in {
    //given
    val key: K = genRedisKey.sample.get
    val offset: Long = genLong.sample.get
    val value: V = genLong.sample.get.toInt
    when(asyncRedisCommands.setbit(key, offset, value)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.setbit(key, offset, value)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).setbit(key, offset, value)
  }

  it should "implement setex" in {
    //given
    val key: K = genRedisKey.sample.get
    val seconds: Long = genLong.sample.get
    val value: V = genLong.sample.get.toInt
    when(asyncRedisCommands.setex(key, seconds, value)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisString.setex(key, seconds, value)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).setex(key, seconds, value)
  }

  it should "implement psetex" in {
    //given
    val key: K = genRedisKey.sample.get
    val milliseconds: Long = genLong.sample.get
    val value: V = genLong.sample.get.toInt
    when(asyncRedisCommands.psetex(key, milliseconds, value)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisString.psetex(key, milliseconds, value)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).psetex(key, milliseconds, value)
  }

  it should "implement setnx" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: V = genLong.sample.get.toInt
    when(asyncRedisCommands.setnx(key, value)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisString.setnx(key, value)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).setnx(key, value)
  }

  it should "implement setrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val offset: Long = genLong.sample.get
    val value: V = genLong.sample.get.toInt
    when(asyncRedisCommands.setrange(key, offset, value)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.setrange(key, offset, value)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).setrange(key, offset, value)
  }

  it should "implement strlen" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.strlen(key)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisString.strlen(key)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).strlen(key)
  }

}
