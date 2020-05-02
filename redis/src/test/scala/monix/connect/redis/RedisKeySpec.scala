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

import java.time.Instant
import java.util.Date

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.{KeyScanCursor, KeyValue, MapScanCursor, ScanArgs, ScanCursor}
import monix.eval.Task
import monix.reactive.Observable
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

class RedisKeySpec
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

  s"${RedisKey} " should "extend the RedisKey trait" in {
    RedisKey shouldBe a[RedisKey]
  }

  it should "implement del operation" in {
    //given
    val k: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.del(k: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisKey.del(k: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).del(k: _*)
  }

  it should "implement unlink operation" in {
    //given
    val k: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.unlink(k: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisKey.unlink(k: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).unlink(k: _*)
  }

  it should "implement dump operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(asyncRedisCommands.dump(k)).thenReturn(MockRedisFuture[Array[Byte]])

    //when
    val t = RedisKey.dump(k)

    //then
    t shouldBe a[Task[Array[Byte]]]
    verify(asyncRedisCommands).dump(k)
  }

  it should "implement exists operation" in {
    //given
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.exists(keys: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisKey.exists(keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).exists(keys: _*)
  }

  it should "implement expire operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val seconds: Long = genLong.sample.get
    when(asyncRedisCommands.expire(k, seconds)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisKey.expire(k, seconds)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).expire(k, seconds)
  }

  it should "implement expireat operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val date: Date = new Date()
    val timestamp: Long = Instant.now.toEpochMilli
    when(asyncRedisCommands.expireat(k, date)).thenReturn(MockRedisFuture[java.lang.Boolean])
    when(asyncRedisCommands.expireat(k, timestamp)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val r1 = RedisKey.expireat(k, date)
    val r2 = RedisKey.expireat(k, timestamp)

    //then
    r1 shouldBe a[Task[Boolean]]
    r2 shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).expireat(k, date)
    verify(asyncRedisCommands).expireat(k, timestamp)
  }

  it should "implement keys operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.keys(k)).thenReturn(MockFlux[K])

    //when
    val t = RedisKey.keys(k)

    //then
    t shouldBe a[Observable[K]]
    verify(reactiveRedisCommands).keys(k)
  }

  it should "implement migrate operation" in {
    //given
    val host: String = Gen.alphaLowerStr.sample.get
    val port = genInt.sample.get
    val k = genRedisKey.sample.get
    val db = genInt.sample.get
    val timestamp: Long = Instant.now.toEpochMilli
    when(asyncRedisCommands.migrate(host, port, k, db, timestamp)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisKey.migrate(host, port, k, db, timestamp)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).migrate(host, port, k, db, timestamp)
  }

  it should "implement move operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val db: Int = genInt.sample.get
    when(asyncRedisCommands.move(k, db)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisKey.move(k, db)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).move(k, db)
  }

  it should "implement objectEncoding operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(asyncRedisCommands.objectEncoding(k)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisKey.objectEncoding(k)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).objectEncoding(k)
  }

  it should "implement objectIdletime operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(asyncRedisCommands.objectIdletime(k)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisKey.objectIdletime(k)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).objectIdletime(k)
  }

  it should "implement objectRefcount operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(asyncRedisCommands.objectRefcount(k)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisKey.objectRefcount(k)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).objectRefcount(k)
  }

  it should "implement persist operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(asyncRedisCommands.persist(k)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisKey.persist(k)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).persist(k)
  }

  it should "implement pexpire operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val milliseconds: Long = genLong.sample.get
    when(asyncRedisCommands.pexpire(k, milliseconds)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisKey.pexpire(k, milliseconds)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).pexpire(k, milliseconds)
  }

  it should "implement pexpireat operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val date: Date = new Date()
    val timestamp: Long = Instant.now.toEpochMilli
    when(asyncRedisCommands.pexpireat(k, date)).thenReturn(MockRedisFuture[java.lang.Boolean])
    when(asyncRedisCommands.pexpireat(k, timestamp)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val r1 = RedisKey.pexpireat(k, date)
    val r2 = RedisKey.pexpireat(k, timestamp)

    //then
    r1 shouldBe a[Task[Boolean]]
    r2 shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).pexpireat(k, date)
    verify(asyncRedisCommands).pexpireat(k, timestamp)
  }

  it should "implement pttl operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(asyncRedisCommands.pttl(k)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisKey.pttl(k)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).pttl(k)
  }

  it should "implement randomkey operation" in {
    //given
    when(asyncRedisCommands.randomkey()).thenReturn(MockRedisFuture[Int])

    //when
    val t = RedisKey.randomkey()

    //then
    t shouldBe a[Task[Int]]
    verify(asyncRedisCommands).randomkey()
  }

  it should "implement rename operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val newKey: K = genRedisKey.sample.get
    when(asyncRedisCommands.rename(k, newKey)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisKey.rename(k, newKey)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).rename(k, newKey)
  }

  it should "implement renamenx operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val newKey: K = genRedisKey.sample.get
    when(asyncRedisCommands.renamenx(k, newKey)).thenReturn(MockRedisFuture[java.lang.Boolean])

    //when
    val t = RedisKey.renamenx(k, newKey)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).renamenx(k, newKey)
  }

  it should "implement restore operation" in {
    //given
    val k: K = genRedisKey.sample.get
    val ttl: Long = genLong.sample.get
    val v: Array[Byte] = genBytes.sample.get
    when(asyncRedisCommands.restore(k, ttl, v)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisKey.restore(k, ttl, v)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).restore(k, ttl, v)
  }

  it should "implement sort operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(reactiveRedisCommands.sort(k)).thenReturn(MockFlux[V])

    //when
    val t = RedisKey.sort(k)

    //then
    t shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).sort(k)
  }

  it should "implement touch operation" in {
    //given
    val k: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.touch(k: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisKey.touch(k: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).touch(k: _*)
  }

  it should "implement ttl operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(asyncRedisCommands.ttl(k)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisKey.ttl(k)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).ttl(k)
  }

  it should "implement `type` operation" in {
    //given
    val k: K = genRedisKey.sample.get
    when(asyncRedisCommands.`type`(k)).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisKey.`type`(k)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).`type`(k)
  }

  it should "implement scan operation" in {
    //given
    val scanCursor = new ScanCursor()
    when(asyncRedisCommands.scan()).thenReturn(MockRedisFuture[KeyScanCursor[K]])
    when(asyncRedisCommands.scan(scanCursor)).thenReturn(MockRedisFuture[KeyScanCursor[K]])

    //when
    val r1 = RedisKey.scan()
    val r2 = RedisKey.scan(scanCursor)

    //then
    r1 shouldBe a[Task[KeyScanCursor[K]]]
    r2 shouldBe a[Task[KeyScanCursor[K]]]
    verify(asyncRedisCommands).scan()
    verify(asyncRedisCommands).scan(scanCursor)
  }
}
