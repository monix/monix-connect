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

import io.lettuce.core.{KeyValue, MapScanCursor, RedisFuture, ScanArgs, ScanCursor}
import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import monix.reactive.Observable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar.verify
import org.mockito.MockitoSugar.when
import org.mockito.IdiomaticMockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import collection.JavaConverters._

class RedisHashSpec
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

  s"${RedisHash} " should "extend the RedisHash trait" in {
    RedisHash shouldBe a[RedisHash]
  }

  it should "implement hexists operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    when(reactiveRedisCommands.hexists(k, field)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisHash.hexists(k, field)

    //then
    verify(reactiveRedisCommands).hexists(k, field)
  }

  it should "implement hget operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    when(reactiveRedisCommands.hget(k, field)).thenReturn(mockMono[V])

    //when
    val _: Task[Option[V]] = RedisHash.hget(k, field)

    //then
    verify(reactiveRedisCommands).hget(k, field)
  }

  it should "implement hincrby operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val v: Int = genRedisValue.sample.get

    when(reactiveRedisCommands.hincrby(k, field, v)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisHash.hincrby(k, field, v)

    //then
    verify(reactiveRedisCommands).hincrby(k, field, v)
  }

  it should "implement hincrbyfloat operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val v: Int = genRedisValue.sample.get

    when(reactiveRedisCommands.hincrbyfloat(k, field, v)).thenReturn(mockMono[java.lang.Double])

    //when
    val _: Task[Double] = RedisHash.hincrbyfloat(k, field, v)

    //then
    verify(reactiveRedisCommands).hincrbyfloat(k, field, v)
  }

  it should "implement hgetall operation" in {
    //given
    val k: String = genRedisKey.sample.get
    when(reactiveRedisCommands.hgetall(k)).thenReturn(mockMono[java.util.Map[String, Int]])

    //when
    val _: Task[Map[String, V]] = RedisHash.hgetall(k)

    //then
    verify(reactiveRedisCommands).hgetall(k)
  }

  it should "implement hkeys operation" in {
    //given
    val k: String = genRedisKey.sample.get
    when(reactiveRedisCommands.hkeys(k)).thenReturn(mockFlux[String])

    //when
    val _: Observable[String] = RedisHash.hkeys(k)

    //then
    verify(reactiveRedisCommands).hkeys(k)
  }

  it should "implement hlen operation" in {
    //given
    val k: String = genRedisKey.sample.get
    when(reactiveRedisCommands.hlen(k)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisHash.hlen(k)

    //then
    verify(reactiveRedisCommands).hlen(k)
  }

  it should "implement hmget operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val fields: List[String] = genRedisKeys.sample.get
    when(reactiveRedisCommands.hmget(k, fields: _*)).thenReturn(mockFlux[KeyValue[String, Int]])

    //when
    val _: Observable[KeyValue[String, V]] = RedisHash.hmget(k, fields: _*)

    //then
    verify(reactiveRedisCommands).hmget(k, fields: _*)
  }

  it should "implement hmset operation" in {

    //given
    val n = 5
    val k: String = genRedisKey.sample.get
    val map: Map[String, Int] = genRedisKeys.sample.get.zip(genRedisValues.sample.get).toMap
    when(reactiveRedisCommands.hmset(k, map.asJava)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisHash.hmset(k, map)

    //then
    verify(reactiveRedisCommands).hmset(k, map.asJava)
  }

  it should "implement hscan operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val scanCursor = mock[ScanCursor]

    when(reactiveRedisCommands.hscan(k)).thenReturn(mockMono[MapScanCursor[String, Int]])
    when(reactiveRedisCommands.hscan(k, scanCursor)).thenReturn(mockMono[MapScanCursor[String, Int]])

    //when
    val t1: Task[MapScanCursor[String, V]] = RedisHash.hscan(k)
    val t2: Task[MapScanCursor[String, V]] = RedisHash.hscan(k, scanCursor)

    //then
    t1.isInstanceOf[Task[MapScanCursor[String, Int]]] shouldBe true
    t2.isInstanceOf[Task[MapScanCursor[String, Int]]] shouldBe true

    verify(reactiveRedisCommands).hscan(k)
    verify(reactiveRedisCommands).hscan(k, scanCursor)
  }

  it should "implement hset" in {
    //given
    val key: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val value: Int = genRedisValue.sample.get
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    when(reactiveRedisCommands.hset(key, field, value)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisHash.hset(key, field, value)

    //then
    verify(reactiveRedisCommands).hset(key, field, value)
  }

  it should "implement hsetnx" in {
    //given
    val key: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val value: Int = genRedisValue.sample.get
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    when(reactiveRedisCommands.hsetnx(key, field, value)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisHash.hsetnx(key, field, value)

    //then
    verify(reactiveRedisCommands).hsetnx(key, field, value)
  }

  it should "implement hstrlen" in {
    //given
    val key: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    when(reactiveRedisCommands.hstrlen(key, field)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisHash.hstrlen(key, field)

    //then
    verify(reactiveRedisCommands).hstrlen(key, field)
  }

  it should "implement hvals operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val v: Int = genRedisValue.sample.get

    when(reactiveRedisCommands.hincrby(k, field, v)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisHash.hincrby(k, field, v)

    //then
    verify(reactiveRedisCommands).hincrby(k, field, v)
  }

}
