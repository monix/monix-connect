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

class RedisKeySpec
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

  s"${RedisKey} " should "extend the RedisKey trait" in {
    RedisKey shouldBe a[RedisKey]
  }

  it should "implement del operation" in {
    //given
    val k: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.del(k: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisKey.del(k: _*)

    //then
    verify(reactiveRedisCommands).del(k: _*)
  }

  it should "implement unlink operation" in {
    //given
    val k: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.unlink(k: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisKey.unlink(k: _*)

    //then
    verify(reactiveRedisCommands).unlink(k: _*)
  }

  it should "implement dump operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.dump(k)).thenReturn(mockMono[Array[Byte]])

    //when
    val _: Task[Array[Byte]] = RedisKey.dump(k)

    //then
    verify(reactiveRedisCommands).dump(k)
  }

  it should "implement exists operation" in {
    //given
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.exists(keys: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisKey.exists(keys: _*)

    //then
    verify(reactiveRedisCommands).exists(keys: _*)
  }

  it should "implement expire operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val seconds: Long = genLong.sample.get
    when(reactiveRedisCommands.expire(k, seconds)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisKey.expire(k, seconds)

    //then
    verify(reactiveRedisCommands).expire(k, seconds)
  }

  it should "implement expireat operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val date: Date = new Date()
    val timestamp: Long = Instant.now.toEpochMilli
    when(reactiveRedisCommands.expireat(k, date)).thenReturn(mockMono[java.lang.Boolean])
    when(reactiveRedisCommands.expireat(k, timestamp)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val r1: Task[Boolean] = RedisKey.expireat(k, date)
    val r2: Task[Boolean] = RedisKey.expireat(k, timestamp)

    //then
    r1.isInstanceOf[Task[Boolean]] shouldBe true
    r2.isInstanceOf[Task[Boolean]] shouldBe true
    verify(reactiveRedisCommands).expireat(k, date)
    verify(reactiveRedisCommands).expireat(k, timestamp)
  }

  it should "implement keys operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.keys(k)).thenReturn(mockFlux[K])

    //when
    val _: Observable[K] = RedisKey.keys(k)

    //then
    verify(reactiveRedisCommands).keys(k)
  }

  it should "implement migrate operation" in {
    //given
    val host: String = Gen.alphaLowerStr.sample.get
    val port = genInt.sample.get
    val k = genRedisKey.sample.get
    val db = genInt.sample.get
    val timestamp: Long = Instant.now.toEpochMilli
    when(reactiveRedisCommands.migrate(host, port, k, db, timestamp)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisKey.migrate(host, port, k, db, timestamp)

    //then
    verify(reactiveRedisCommands).migrate(host, port, k, db, timestamp)
  }

  it should "implement move operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val db: Int = genInt.sample.get
    when(reactiveRedisCommands.move(k, db)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisKey.move(k, db)

    //then
    verify(reactiveRedisCommands).move(k, db)
  }

  it should "implement objectEncoding operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.objectEncoding(k)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisKey.objectEncoding(k)

    //then
    verify(reactiveRedisCommands).objectEncoding(k)
  }

  it should "implement objectIdletime operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.objectIdletime(k)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisKey.objectIdletime(k)

    //then
    verify(reactiveRedisCommands).objectIdletime(k)
  }

  it should "implement objectRefcount operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.objectRefcount(k)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisKey.objectRefcount(k)

    //then
    verify(reactiveRedisCommands).objectRefcount(k)
  }

  it should "implement persist operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.persist(k)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisKey.persist(k)

    //then
    verify(reactiveRedisCommands).persist(k)
  }

  it should "implement pexpire operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val milliseconds: Long = genLong.sample.get
    when(reactiveRedisCommands.pexpire(k, milliseconds)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisKey.pexpire(k, milliseconds)

    //then
    verify(reactiveRedisCommands).pexpire(k, milliseconds)
  }

  it should "implement pexpireat operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val date: Date = new Date()
    val timestamp: Long = Instant.now.toEpochMilli
    when(reactiveRedisCommands.pexpireat(k, date)).thenReturn(mockMono[java.lang.Boolean])
    when(reactiveRedisCommands.pexpireat(k, timestamp)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val r1: Task[Boolean] = RedisKey.pexpireat(k, date)
    val r2: Task[Boolean] = RedisKey.pexpireat(k, timestamp)

    //then
    r1.isInstanceOf[Task[Boolean]] shouldBe true
    r2.isInstanceOf[Task[Boolean]] shouldBe true
    verify(reactiveRedisCommands).pexpireat(k, date)
    verify(reactiveRedisCommands).pexpireat(k, timestamp)
  }

  it should "implement pttl operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.pttl(k)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisKey.pttl(k)

    //then
    verify(reactiveRedisCommands).pttl(k)
  }

  it should "implement randomkey operation" in {
    //given
    when(reactiveRedisCommands.randomkey()).thenReturn(mockMono[String])

    //when
    val _: Task[V] = RedisKey.randomkey()

    //then
    verify(reactiveRedisCommands).randomkey()
  }

  it should "implement rename operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val newKey: K = genRedisKey.sample.get
    when(reactiveRedisCommands.rename(k, newKey)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisKey.rename(k, newKey)

    //then
    verify(reactiveRedisCommands).rename(k, newKey)
  }

  it should "implement renamenx operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val newKey: K = genRedisKey.sample.get
    when(reactiveRedisCommands.renamenx(k, newKey)).thenReturn(mockMono[java.lang.Boolean])

    //when
    val _: Task[Boolean] = RedisKey.renamenx(k, newKey)

    //then
    verify(reactiveRedisCommands).renamenx(k, newKey)
  }

  it should "implement restore operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val ttl: Long = genLong.sample.get
    val v: Array[Byte] = genBytes.sample.get
    when(reactiveRedisCommands.restore(k, ttl, v)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisKey.restore(k, ttl, v)

    //then
    verify(reactiveRedisCommands).restore(k, ttl, v)
  }

  it should "implement sort operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.sort(k)).thenReturn(mockFlux[V])

    //when
    val _: Observable[V] = RedisKey.sort(k)

    //then
    verify(reactiveRedisCommands).sort(k)
  }

  it should "implement touch operation" in {
    //given
    val k: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.touch(k: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisKey.touch(k: _*)

    //then
    verify(reactiveRedisCommands).touch(k: _*)
  }

  it should "implement ttl operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.ttl(k)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisKey.ttl(k)

    //then
    verify(reactiveRedisCommands).ttl(k)
  }

  it should "implement `type` operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.`type`(k)).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisKey.`type`(k)

    //then
    verify(reactiveRedisCommands).`type`(k)
  }

  it should "implement scan operation" in {
    //given
    val scanCursor = new ScanCursor()
    when(reactiveRedisCommands.scan()).thenReturn(mockMono[KeyScanCursor[K]])
    when(reactiveRedisCommands.scan(scanCursor)).thenReturn(mockMono[KeyScanCursor[K]])

    //when
    val r1: Task[KeyScanCursor[String]] = RedisKey.scan()
    val r2: Task[KeyScanCursor[String]] = RedisKey.scan(scanCursor)

    //then
    r1.isInstanceOf[Task[KeyScanCursor[String]]] shouldBe true
    r2.isInstanceOf[Task[KeyScanCursor[String]]] shouldBe true
    verify(reactiveRedisCommands).scan()
    verify(reactiveRedisCommands).scan(scanCursor)
  }
}
