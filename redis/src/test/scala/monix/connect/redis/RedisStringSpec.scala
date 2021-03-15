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

import scala.jdk.CollectionConverters._

class RedisStringSpec
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

  s"${RedisString} " should " implement RedisString trait" in {
    RedisString shouldBe a[RedisString]
  }

  it should "implement append operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val v: V = genRedisValue.sample.get
    when(reactiveRedisCommands.append(k, v)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = Redis.append(k, v)

    //then
    verify(reactiveRedisCommands).append(k, v)
  }

  it should "implement bitcount" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.bitcount(key)).thenReturn(mockMono[java.lang.Long])
    when(reactiveRedisCommands.bitcount(key, start, stop)).thenReturn(mockMono[java.lang.Long])

    //when
    val r1: Task[Long] = Redis.bitcount(key)
    val r2: Task[Long] = Redis.bitcount(key, start, stop)

    //then
    r1.isInstanceOf[Task[Long]] shouldBe true
    r2.isInstanceOf[Task[Long]] shouldBe true
    verify(reactiveRedisCommands).bitcount(key)
    verify(reactiveRedisCommands).bitcount(key, start, stop)
  }

  it should "implement bitpos" in {
    //given
    val key: K = genRedisKey.sample.get
    val state: Boolean = genBool.sample.get
    val start: Long = genLong.sample.get
    val end: Long = genLong.sample.get
    when(reactiveRedisCommands.bitpos(key, state)).thenReturn(mockMono[java.lang.Long])
    when(reactiveRedisCommands.bitpos(key, state, start)).thenReturn(mockMono[java.lang.Long])
    when(reactiveRedisCommands.bitpos(key, state, start, end)).thenReturn(mockMono[java.lang.Long])

    //when
    val r1: Task[Long] = Redis.bitpos(key, state)
    val r2: Task[Long] = Redis.bitpos(key, state, start)
    val r3: Task[Long] = Redis.bitpos(key, state, start, end)

    //then
    r1.isInstanceOf[Task[Long]] shouldBe true
    r2.isInstanceOf[Task[Long]] shouldBe true
    r3.isInstanceOf[Task[Long]] shouldBe true
    verify(reactiveRedisCommands).bitpos(key, state)
    verify(reactiveRedisCommands).bitpos(key, state, start)
    verify(reactiveRedisCommands).bitpos(key, state, start, end)
  }

  it should "implement bitopAnd" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.bitopAnd(dest, keys: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = Redis.bitopAnd(dest, keys: _*)

    //then
    verify(reactiveRedisCommands).bitopAnd(dest, keys: _*)
  }

  it should "implement bitopNot" in {
    //given
    val dest: K = genRedisKey.sample.get
    val source: K = genRedisKey.sample.get
    when(reactiveRedisCommands.bitopNot(dest, source)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = Redis.bitopNot(dest, source)

    //then
    verify(reactiveRedisCommands).bitopNot(dest, source)
  }

  it should "implement bitopOr" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.bitopOr(dest, keys: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = Redis.bitopOr(dest, keys: _*)

    //then
    verify(reactiveRedisCommands).bitopOr(dest, keys: _*)
  }

  it should "implement bitopXor" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.bitopXor(dest, keys: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.bitopXor(dest, keys: _*)

    //then
    verify(reactiveRedisCommands).bitopXor(dest, keys: _*)
  }

  it should "implement decr" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.decr(key)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.decr(key)

    //then
    verify(reactiveRedisCommands).decr(key)
  }

  it should "implement decrby" in {
    //given
    val key: K = genRedisKey.sample.get
    val amount: Long = genLong.sample.get
    when(reactiveRedisCommands.decrby(key, amount)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.decrby(key, amount)

    //then
    verify(reactiveRedisCommands).decrby(key, amount)
  }

  it should "implement get" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.get(key)).thenReturn(mockMono[Int])

    //when
    val _: Task[V] = RedisString.get(key)

    //then
    verify(reactiveRedisCommands).get(key)
  }

  it should "implement getbit" in {
    //given
    val key: K = genRedisKey.sample.get
    val offset: Long = genLong.sample.get
    when(reactiveRedisCommands.getbit(key, offset)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.getbit(key, offset)

    //then
    verify(reactiveRedisCommands).getbit(key, offset)
  }

  it should "implement getrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val end: Long = genLong.sample.get
    when(reactiveRedisCommands.getrange(key, start, end)).thenReturn(mockMono[V])

    //when
    val _: Task[V] = RedisString.getrange(key, start, end)

    //then
    verify(reactiveRedisCommands).getrange(key, start, end)
  }

  it should "implement getset" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: V = genRedisValue.sample.get
    when(reactiveRedisCommands.getset(key, value)).thenReturn(mockMono[V])

    //when
    val _: Task[V] = RedisString.getset(key, value)

    //then
    verify(reactiveRedisCommands).getset(key, value)
  }

  it should "implement incr" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.incr(key)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.incr(key)

    //then
    verify(reactiveRedisCommands).incr(key)
  }

  it should "implement incrby" in {
    //given
    val key: K = genRedisKey.sample.get
    val amount: Long = genLong.sample.get
    when(reactiveRedisCommands.incrby(key, amount)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.incrby(key, amount)

    //then
    verify(reactiveRedisCommands).incrby(key, amount)
  }

  it should "implement incrbyfloat" in {
    //given
    val key: K = genRedisKey.sample.get
    val amount: Double = genLong.sample.get.toDouble
    when(reactiveRedisCommands.incrbyfloat(key, amount)).thenReturn(mockMono[java.lang.Double])

    //when
    val _: Task[Double] = RedisString.incrbyfloat(key, amount)

    //then
    verify(reactiveRedisCommands).incrbyfloat(key, amount)
  }

  it should "implement mget" in {
    //given
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.mget(keys: _*)).thenReturn(mockFlux[KeyValue[K, V]])

    //when
    val _: Observable[KeyValue[K, V]] = RedisString.mget(keys: _*)

    //then
    verify(reactiveRedisCommands).mget(keys: _*)
  }

  it should "implement mset" in {
    //given
    val map: Map[K, V] = genKvMap.sample.get
    when(reactiveRedisCommands.mset(map.asJava)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisString.mset(map)

    //then
    verify(reactiveRedisCommands).mset(map.asJava)
  }

  it should "implement msetnx" in {
    //given
    val map: Map[K, V] = genKvMap.sample.get
    when(reactiveRedisCommands.msetnx(map.asJava)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisString.msetnx(map)

    //then
    verify(reactiveRedisCommands).msetnx(map.asJava)
  }

  it should "implement set" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: V = genRedisValue.sample.get
    when(reactiveRedisCommands.set(key, value)).thenReturn(mockMono[String])
    //with setArgs not supported

    //when
    val _: Task[String] = RedisString.set(key, value)

    //then
    verify(reactiveRedisCommands).set(key, value)
  }

  it should "implement setbit" in {
    //given
    val key: K = genRedisKey.sample.get
    val offset: Long = genLong.sample.get
    val value: V = genLong.sample.get.toInt
    when(reactiveRedisCommands.setbit(key, offset, value)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.setbit(key, offset, value)

    //then
    verify(reactiveRedisCommands).setbit(key, offset, value)
  }

  it should "implement setex" in {
    //given
    val key: K = genRedisKey.sample.get
    val seconds: Long = genLong.sample.get
    val value: V = genLong.sample.get.toInt
    when(reactiveRedisCommands.setex(key, seconds, value)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisString.setex(key, seconds, value)

    //then
    verify(reactiveRedisCommands).setex(key, seconds, value)
  }

  it should "implement psetex" in {
    //given
    val key: K = genRedisKey.sample.get
    val milliseconds: Long = genLong.sample.get
    val value: V = genLong.sample.get.toInt
    when(reactiveRedisCommands.psetex(key, milliseconds, value)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisString.psetex(key, milliseconds, value)

    //then
    verify(reactiveRedisCommands).psetex(key, milliseconds, value)
  }

  it should "implement setnx" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: V = genLong.sample.get.toInt
    when(reactiveRedisCommands.setnx(key, value)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisString.setnx(key, value)

    //then
    verify(reactiveRedisCommands).setnx(key, value)
  }

  it should "implement setrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val offset: Long = genLong.sample.get
    val value: V = genLong.sample.get.toInt
    when(reactiveRedisCommands.setrange(key, offset, value)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.setrange(key, offset, value)

    //then
    verify(reactiveRedisCommands).setrange(key, offset, value)
  }

  it should "implement strlen" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.strlen(key)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisString.strlen(key)

    //then
    verify(reactiveRedisCommands).strlen(key)
  }

}
