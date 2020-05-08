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
import reactor.core.publisher.Flux

import scala.jdk.CollectionConverters._

class RedisHashSpec
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

  s"${RedisHash} " should "extend the RedisHash trait" in {
    RedisHash shouldBe a[RedisHash]
  }

  it should "implement hexists operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    when(asyncRedisCommands.hexists(k, field)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisHash.hexists(k, field)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).hexists(k, field)
  }

  it should "implement hget operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    when(asyncRedisCommands.hget(k, field)).thenReturn(MockRedisFuture[V])

    //when
    val t = RedisHash.hget(k, field)

    //then
    t shouldBe a[Task[Int]]
    verify(asyncRedisCommands).hget(k, field)
  }

  it should "implement hincrby operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val v: Int = genRedisValue.sample.get

    when(asyncRedisCommands.hincrby(k, field, v)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisHash.hincrby(k, field, v)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).hincrby(k, field, v)
  }

  it should "implement hincrbyfloat operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val v: Int = genRedisValue.sample.get

    when(asyncRedisCommands.hincrbyfloat(k, field, v)).thenReturn(MockRedisFuture[java.lang.Double])

    //when
    val t = RedisHash.hincrbyfloat(k, field, v)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).hincrbyfloat(k, field, v)
  }

  it should "implement hgetall operation" in {
    //given
    val k: String = genRedisKey.sample.get
    when(asyncRedisCommands.hgetall(k)).thenReturn(MockRedisFuture[java.util.Map[String, Int]])

    //when
    val t = RedisHash.hgetall(k)

    //then
    t shouldBe a[Task[Map[String, String]]]
    verify(asyncRedisCommands).hgetall(k)
  }

  it should "implement hkeys operation" in {
    //given
    val k: String = genRedisKey.sample.get
    when(reactiveRedisCommands.hkeys(k)).thenReturn(MockFlux[String])

    //when
    val t = RedisHash.hkeys(k)

    //then
    t shouldBe a[Observable[String]]
    verify(reactiveRedisCommands).hkeys(k)
  }

  it should "implement hlen operation" in {
    //given
    val k: String = genRedisKey.sample.get
    when(asyncRedisCommands.hlen(k)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisHash.hlen(k)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).hlen(k)
  }

  it should "implement hmget operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val fields: List[String] = genRedisKeys.sample.get
    when(reactiveRedisCommands.hmget(k, fields: _*)).thenReturn(MockFlux[KeyValue[String, Int]])

    //when
    val ob = RedisHash.hmget(k, fields: _*)

    //then
    ob shouldBe a[Observable[KeyValue[String, Int]]]
    verify(reactiveRedisCommands).hmget(k, fields: _*)
  }

  it should "implement hmset operation" in {

    //given
    val n = 5
    val k: String = genRedisKey.sample.get
    val map: Map[String, Int] = genRedisKeys.sample.get.zip(genRedisValues.sample.get).toMap
    when(asyncRedisCommands.hmset(k, map.asJava)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisHash.hmset(k, map)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).hmset(k, map.asJava)
  }

  it should "implement hscan operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val scanCursor = mock[ScanCursor]

    when(asyncRedisCommands.hscan(k)).thenReturn(MockRedisFuture[MapScanCursor[String, Int]])
    when(asyncRedisCommands.hscan(k, scanCursor)).thenReturn(MockRedisFuture[MapScanCursor[String, Int]])

    //when
    val t1 = RedisHash.hscan(k)
    val t2 = RedisHash.hscan(k, scanCursor)

    //then
    t1 shouldBe a[Task[MapScanCursor[String, Int]]]
    t2 shouldBe a[Task[MapScanCursor[String, Int]]]

    verify(asyncRedisCommands).hscan(k)
    verify(asyncRedisCommands).hscan(k, scanCursor)
  }

  it should "implement hset" in {
    //given
    val key: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val value: Int = genRedisValue.sample.get
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(asyncRedisCommands.hset(key, field, value)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisHash.hset(key, field, value)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).hset(key, field, value)
  }

  it should "implement hsetnx" in {
    //given
    val key: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val value: Int = genRedisValue.sample.get
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(asyncRedisCommands.hsetnx(key, field, value)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisHash.hsetnx(key, field, value)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).hsetnx(key, field, value)
  }

  it should "implement hstrlen" in {
    //given
    val key: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(asyncRedisCommands.hstrlen(key, field)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisHash.hstrlen(key, field)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).hstrlen(key, field)
  }

  it should "implement hvals operation" in {
    //given
    val k: String = genRedisKey.sample.get
    val field: String = genRedisKey.sample.get
    val v: Int = genRedisValue.sample.get

    when(asyncRedisCommands.hincrby(k, field, v)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisHash.hincrby(k, field, v)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).hincrby(k, field, v)
  }

}
