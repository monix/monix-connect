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

import java.lang

import io.lettuce.core.ValueScanCursor
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
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    super.beforeAll()
  }

  override def beforeEach() = {
    reset(reactiveRedisCommands)
    reset(reactiveRedisCommands)
  }
/*
  s"${RedisSet} " should " implement RedisSet trait" in {
    RedisSet shouldBe a[SetCommands]
  }

  it should "implement sadd" in {
    //given
    val key: String = genRedisKey.sample.get
    val members: List[Int] = genRedisValues.sample.get
    when(reactiveRedisCommands.sadd(key, members: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSet.sadd(key, members: _*)

    //then
    verify(reactiveRedisCommands).sadd(key, members: _*)
  }

  it should "implement scard" in {
    //given
    val key: String = genRedisKey.sample.get
    when(reactiveRedisCommands.scard(key)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSet.scard(key)

    //then
    verify(reactiveRedisCommands).scard(key)
  }

  it should "implement sdiff" in {
    //given
    val keys: List[String] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sdiff(keys: _*)).thenReturn(mockFlux[Int])

    //when
    val _: Observable[V] = RedisSet.sdiff(keys: _*)

    //then
    verify(reactiveRedisCommands).sdiff(keys: _*)
  }

  it should "implement sdiffstore" in {
    //given
    val dest: String = genRedisKey.sample.get
    val keys: List[String] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sdiffstore(dest, keys: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSet.sdiffstore(dest, keys: _*)

    //then
    verify(reactiveRedisCommands).sdiffstore(dest, keys: _*)
  }

  it should "implement sinter" in {
    //given
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sinter(keys: _*)).thenReturn(mockFlux[Int])

    //when
    val _: Observable[V] = $Commands.sinter(keys: _*)

    //then
    verify(reactiveRedisCommands).sinter(keys: _*)
  }

  it should "implement sinterstore" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sinterstore(dest, keys: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[lang.Long] = $Commands.sinterstore(dest, keys: _*)

    //then
    verify(reactiveRedisCommands).sinterstore(dest, keys: _*)
  }

  it should "implement sismember" in {
    //given
    val key: String = genRedisKey.sample.get
    val member: Int = genRedisValue.sample.get
    when(reactiveRedisCommands.sismember(key, member)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = $Commands.sismember(key, member)

    //then
    verify(reactiveRedisCommands).sismember(key, member)
  }

  it should "implement smove" in {
    //given
    val source: String = genRedisKey.sample.get
    val dest: String = genRedisKey.sample.get
    val member: Int = genRedisValue.sample.get
    when(reactiveRedisCommands.smove(source, dest, member)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = $Commands.smove(source, dest, member)

    //then
    verify(reactiveRedisCommands).smove(source, dest, member)
  }

  it should "implement smembers" in {
    //given
    val key: String = genRedisKey.sample.get
    when(reactiveRedisCommands.smembers(key)).thenReturn(mockFlux[V])

    //when
    val _: Observable[V] = $Commands.smembers(key)

    //then
    verify(reactiveRedisCommands).smembers(key)
  }

  it should "implement spop" in {
    //given
    val key: String = genRedisKey.sample.get
    val count: Long = genLong.sample.get
    when(reactiveRedisCommands.spop(key)).thenReturn(mockMono[V])
    when(reactiveRedisCommands.spop(key, count)).thenReturn(mockFlux[V])

    //when
    val r1: Task[V] = $Commands.spop(key)
    val r2: Observable[V] = RedisSet.spop(key, count)

    //then
    r1.isInstanceOf[Task[V]] shouldBe true
    r2.isInstanceOf[Observable[V]] shouldBe true
    verify(reactiveRedisCommands).spop(key)
    verify(reactiveRedisCommands).spop(key, count)
  }

  it should "implement srandmember" in {
    //given
    val key: String = genRedisKey.sample.get
    val count: Long = genLong.sample.get
    when(reactiveRedisCommands.srandmember(key)).thenReturn(mockMono[V])
    when(reactiveRedisCommands.srandmember(key, count)).thenReturn(mockFlux[V])

    //when
    val r1: Task[V] = $Commands.srandmember(key)
    val r2: Observable[V] = $Commands.srandmember(key, count)

    //then
    r1.isInstanceOf[Task[V]] shouldBe true
    r2.isInstanceOf[Observable[V]] shouldBe true
    verify(reactiveRedisCommands).srandmember(key)
    verify(reactiveRedisCommands).srandmember(key, count)
  }

  it should "implement srem" in {
    //given
    val key: String = genRedisKey.sample.get
    val members: List[V] = genRedisValues.sample.get
    when(reactiveRedisCommands.srem(key, members: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSet.srem(key, members: _*)

    //then
    verify(reactiveRedisCommands).srem(key, members: _*)
  }

  it should "implement sunion" in {
    //given
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sunion(keys: _*)).thenReturn(mockFlux[V])

    //when
    val _: Observable[V] = RedisSet.sunion(keys: _*)

    //then
    verify(reactiveRedisCommands).sunion(keys: _*)
  }

  it should "implement sunionstore" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.sunionstore(dest, keys: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSet.sunionstore(dest, keys: _*)

    //then
    verify(reactiveRedisCommands).sunionstore(dest, keys: _*)
  }

  it should "implement sscan" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.sscan(key)).thenReturn(mockMono[ValueScanCursor[V]])
    //wiht scanArgs and scanCursor not supported

    //when
    val _: Task[ValueScanCursor[V]] = RedisSet.sscan(key)

    //then
    verify(reactiveRedisCommands).sscan(key)
  }*/

}
