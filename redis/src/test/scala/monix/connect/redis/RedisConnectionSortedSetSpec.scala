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

import io.lettuce.core.{KeyValue, Limit, ScoredValue, ScoredValueScanCursor}
import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import monix.reactive.Observable

class RedisConnectionSortedSetSpec
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
  s"${RedisSortedSet} " should "implement the RedisSortedSet trait" in {
    RedisSortedSet shouldBe a[SortedSetCommands]
  }

  it should "implement bzpopmin" in {
    //given
    val timeout: Long = genLong.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.bzpopmin(timeout, keys: _*)).thenReturn(mockMono[KeyValue[K, ScoredValue[V]]])

    //when
    val _: Task[KeyValue[K, ScoredValue[V]]] = RedisSortedSet.bzpopmin(timeout, keys: _*)

    //then
    verify(reactiveRedisCommands).bzpopmin(timeout, keys: _*)
  }

  it should "implement bzpopmax" in {
    //given
    val timeout: Long = genLong.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.bzpopmax(timeout, keys: _*)).thenReturn(mockMono[KeyValue[K, ScoredValue[V]]])

    //when
    val _: Task[KeyValue[K, ScoredValue[V]]] = RedisSortedSet.bzpopmax(timeout, keys: _*)

    //then
    verify(reactiveRedisCommands).bzpopmax(timeout, keys: _*)
  }

  it should "implement zadd" in {
    //given
    val key: K = genRedisKey.sample.get
    val scoredValues: List[ScoredValue[V]] = genScoredValues.sample.get
    when(reactiveRedisCommands.zadd(key, scoredValues: _*)).thenReturn(mockMono[java.lang.Long])
    //with zArgs not supported

    //when
    val _: Task[Long] = RedisSortedSet.zadd(key, scoredValues: _*)

    //then
    verify(reactiveRedisCommands).zadd(key, scoredValues: _*)
  }

  it should "implement zaddincr" in {
    //given
    val key: K = genRedisKey.sample.get
    val score = genDouble.sample.get
    val member = genRedisValue.sample.get
    when(reactiveRedisCommands.zaddincr(key, score, member)).thenReturn(mockMono[java.lang.Double])
    //with zAddArgs not cotemplated

    //when
    val _: Task[Double] = RedisSortedSet.zaddincr(key, score, member)

    //then
    verify(reactiveRedisCommands).zaddincr(key, score, member)
  }

  it should "implement zcard" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.zcard(key)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zcard(key)

    //then
    verify(reactiveRedisCommands).zcard(key)
  }

  it should "implement zcount" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.zcount(key, unboundedRange)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zcount(key, unboundedRange)

    //then
    verify(reactiveRedisCommands).zcount(key, unboundedRange)
  }

  it should "implement zincrby" in {
    //given
    val key: K = genRedisKey.sample.get
    val amount: Double = genDouble.sample.get
    val member: V = genRedisValue.sample.get
    when(reactiveRedisCommands.zincrby(key, amount, member)).thenReturn(mockMono[java.lang.Double])

    //when
    val _: Task[Double] = RedisSortedSet.zincrby(key, amount, member)

    //then
    verify(reactiveRedisCommands).zincrby(key, amount, member)
  }

  it should "implement zinterstore" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.zinterstore(dest, keys: _*)).thenReturn(mockMono[java.lang.Long])
    //with zStoreArgs not supported

    //when
    val _: Task[Long] = RedisSortedSet.zinterstore(dest, keys: _*)

    //then
    verify(reactiveRedisCommands).zinterstore(dest, keys: _*)
  }

  it should "implement zlexcount" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.zlexcount(key, unboundedRange)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zlexcount(key, unboundedRange)

    //then
    verify(reactiveRedisCommands).zlexcount(key, unboundedRange)
  }

  it should "implement zpopmin" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.zpopmin(key)).thenReturn(mockMono[ScoredValue[V]])

    //when
    val _: Task[ScoredValue[V]] = RedisSortedSet.zpopmin(key)

    //then
    verify(reactiveRedisCommands).zpopmin(key)
  }

  it should "implement zpopmax" in {
    //given
    val key: K = genRedisKey.sample.get
    val count: Long = genLong.sample.get
    when(reactiveRedisCommands.zpopmax(key)).thenReturn(mockMono[ScoredValue[V]])
    when(reactiveRedisCommands.zpopmax(key, count)).thenReturn(mockFlux[ScoredValue[V]])

    //when
    val r1: Task[ScoredValue[V]] = RedisSortedSet.zpopmax(key)
    val r2: Observable[ScoredValue[V]] = RedisSortedSet.zpopmax(key, count)

    //then
    r1.isInstanceOf[Task[ScoredValue[V]]] shouldBe true
    r2.isInstanceOf[Observable[ScoredValue[V]]] shouldBe true
    verify(reactiveRedisCommands).zpopmax(key)
    verify(reactiveRedisCommands).zpopmax(key, count)
  }

  it should "implement zrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zrange(key, start, stop)).thenReturn(mockFlux[V])

    //when
    val _: Observable[V] = RedisSortedSet.zrange(key, start, stop)

    //then
    verify(reactiveRedisCommands).zrange(key, start, stop)
  }

  it should "implement zrangeWithScores" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zrangeWithScores(key, start, stop)).thenReturn(mockFlux[ScoredValue[V]])

    //when
    val _: Observable[ScoredValue[V]] = RedisSortedSet.zrangeWithScores(key, start, stop)

    //then
    verify(reactiveRedisCommands).zrangeWithScores(key, start, stop)
  }

  it should "implement zrangebylex" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrangebylex(key, unboundedRange)).thenReturn(mockFlux[V])
    when(reactiveRedisCommands.zrangebylex(key, unboundedRange, limit)).thenReturn(mockFlux[V])

    //when
    val r1: Observable[V] = RedisSortedSet.zrangebylex(key, unboundedRange)
    val r2: Observable[V] = RedisSortedSet.zrangebylex(key, unboundedRange, limit)

    //then
    r1.isInstanceOf[Observable[V]] shouldBe true
    r2.isInstanceOf[Observable[V]] shouldBe true
    verify(reactiveRedisCommands).zrangebylex(key, unboundedRange)
    verify(reactiveRedisCommands).zrangebylex(key, unboundedRange, limit)
  }

  it should "implement zrangebyscore" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrangebyscore(key, unboundedRange)).thenReturn(mockFlux[V])
    when(reactiveRedisCommands.zrangebyscore(key, unboundedRange, limit)).thenReturn(mockFlux[V])

    //when
    val r1: Observable[V] = RedisSortedSet.zrangebyscore(key, unboundedRange)
    val r2: Observable[V] = RedisSortedSet.zrangebyscore(key, unboundedRange, limit)

    //then
    r1.isInstanceOf[Observable[V]] shouldBe true
    r2.isInstanceOf[Observable[V]] shouldBe true
    verify(reactiveRedisCommands).zrangebyscore(key, unboundedRange)
    verify(reactiveRedisCommands).zrangebyscore(key, unboundedRange, limit)
  }

  it should "implement zrangebyscoreWithScores" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrangebyscoreWithScores(key, unboundedRange)).thenReturn(mockFlux[ScoredValue[V]])
    when(reactiveRedisCommands.zrangebyscoreWithScores(key, unboundedRange, limit)).thenReturn(mockFlux[ScoredValue[V]])

    //when
    val r1: Observable[ScoredValue[V]] = RedisSortedSet.zrangebyscoreWithScores(key, unboundedRange)
    val r2: Observable[ScoredValue[V]] = RedisSortedSet.zrangebyscoreWithScores(key, unboundedRange, limit)

    //then
    r1.isInstanceOf[Observable[ScoredValue[V]]] shouldBe true
    r2.isInstanceOf[Observable[ScoredValue[V]]] shouldBe true
    verify(reactiveRedisCommands).zrangebyscoreWithScores(key, unboundedRange)
    verify(reactiveRedisCommands).zrangebyscoreWithScores(key, unboundedRange, limit)

  }

  it should "implement zrank" in {
    //given
    val key: K = genRedisKey.sample.get
    val member: V = genRedisValue.sample.get
    when(reactiveRedisCommands.zrank(key, member)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zrank(key, member)

    //then
    verify(reactiveRedisCommands).zrank(key, member)
  }

  it should "implement zrem" in {
    //given
    val key: K = genRedisKey.sample.get
    val members: List[V] = genRedisValues.sample.get
    when(reactiveRedisCommands.zrem(key, members: _*)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zrem(key, members: _*)

    //then
    verify(reactiveRedisCommands).zrem(key, members: _*)
  }

  it should "implement zremrangebylex" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.zremrangebylex(key, unboundedRange)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zremrangebylex(key, unboundedRange)

    //then
    verify(reactiveRedisCommands).zremrangebylex(key, unboundedRange)
  }

  it should "implement zremrangebyrank" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zremrangebyrank(key, start, stop)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zremrangebyrank(key, start, stop)

    //then
    verify(reactiveRedisCommands).zremrangebyrank(key, start, stop)
  }

  it should "implement zremrangebyscore" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.zremrangebyscore(key, unboundedRange)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zremrangebyscore(key, unboundedRange)

    //then
    verify(reactiveRedisCommands).zremrangebyscore(key, unboundedRange)
  }

  it should "implement zrevrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zrevrange(key, start, stop)).thenReturn(mockFlux[V])

    //when
    val _: Observable[V] = RedisSortedSet.zrevrange(key, start, stop)

    //then
    verify(reactiveRedisCommands).zrevrange(key, start, stop)
  }

  it should "implement zrevrangeWithScores" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zrevrangeWithScores(key, start, stop)).thenReturn(mockFlux[ScoredValue[V]])

    //when
    val _: Observable[ScoredValue[V]] = RedisSortedSet.zrevrangeWithScores(key, start, stop)

    //then
    verify(reactiveRedisCommands).zrevrangeWithScores(key, start, stop)
  }

  it should "implement zrevrangebylex" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrevrangebylex(key, unboundedRange)).thenReturn(mockFlux[V])
    when(reactiveRedisCommands.zrevrangebylex(key, unboundedRange, limit)).thenReturn(mockFlux[V])

    //when
    val r1: Observable[V] = RedisSortedSet.zrevrangebylex(key, unboundedRange)
    val r2: Observable[V] = RedisSortedSet.zrevrangebylex(key, unboundedRange, limit)

    //then
    r1.isInstanceOf[Observable[V]] shouldBe true
    r2.isInstanceOf[Observable[V]] shouldBe true
    verify(reactiveRedisCommands).zrevrangebylex(key, unboundedRange)
    verify(reactiveRedisCommands).zrevrangebylex(key, unboundedRange, limit)
  }

  it should "implement zrevrangebyscore" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrevrangebyscore(key, unboundedRange)).thenReturn(mockFlux[V])
    when(reactiveRedisCommands.zrevrangebyscore(key, unboundedRange, limit)).thenReturn(mockFlux[V])
    //with channel in args not supported

    //when
    val r1 = RedisSortedSet.zrevrangebyscore(key, unboundedRange)
    val r2 = RedisSortedSet.zrevrangebyscore(key, unboundedRange, limit)

    //then
    r1.isInstanceOf[Observable[V]] shouldBe true
    r2.isInstanceOf[Observable[V]] shouldBe true
    verify(reactiveRedisCommands).zrevrangebyscore(key, unboundedRange)
    verify(reactiveRedisCommands).zrevrangebyscore(key, unboundedRange, limit)
  }

  it should "implement zrevrangebyscoreWithScores" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrevrangebyscoreWithScores(key, unboundedRange)).thenReturn(mockFlux[ScoredValue[V]])
    when(reactiveRedisCommands.zrevrangebyscoreWithScores(key, unboundedRange, limit))
      .thenReturn(mockFlux[ScoredValue[V]])
    //with channel in args not supported

    //when
    val r1: Observable[ScoredValue[V]] = RedisSortedSet.zrevrangebyscoreWithScores(key, unboundedRange)
    val r2: Observable[ScoredValue[V]] = RedisSortedSet.zrevrangebyscoreWithScores(key, unboundedRange, limit)

    //then
    r1.isInstanceOf[Observable[ScoredValue[V]]] shouldBe true
    r2.isInstanceOf[Observable[ScoredValue[V]]] shouldBe true
    verify(reactiveRedisCommands).zrevrangebyscoreWithScores(key, unboundedRange)
    verify(reactiveRedisCommands).zrevrangebyscoreWithScores(key, unboundedRange, limit)
  }

  it should "implement zrevrank" in {
    //given
    val key: K = genRedisKey.sample.get
    val member: V = genRedisValue.sample.get
    when(reactiveRedisCommands.zrevrank(key, member)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisSortedSet.zrevrank(key, member)

    //then
    verify(reactiveRedisCommands).zrevrank(key, member)
  }

  it should "implement zscan" in {
    //given
    val key: K = genRedisKey.sample.get
    when(reactiveRedisCommands.zscan(key)).thenReturn(mockMono[ScoredValueScanCursor[V]])
    //with scanArgs and scanCursor not supported

    //when
    val _: Task[ScoredValueScanCursor[V]] = RedisSortedSet.zscan(key)

    //then
    verify(reactiveRedisCommands).zscan(key)
  }

  it should "implement zscore" in {
    //given
    val key: K = genRedisKey.sample.get
    val member: V = genRedisValue.sample.get
    when(reactiveRedisCommands.zscore(key, member)).thenReturn(mockMono[java.lang.Double])

    //when
    val _: Task[Double] = RedisSortedSet.zscore(key, member)

    //then
    verify(reactiveRedisCommands).zscore(key, member)
  }

  it should "implement zunionstore" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(reactiveRedisCommands.zunionstore(dest, keys: _*)).thenReturn(mockMono[java.lang.Long])
    //with zStoreArgs not supported

    //when
    val _: Task[Long] = RedisSortedSet.zunionstore(dest, keys: _*)

    //then
    verify(reactiveRedisCommands).zunionstore(dest, keys: _*)
  }
 */
}
