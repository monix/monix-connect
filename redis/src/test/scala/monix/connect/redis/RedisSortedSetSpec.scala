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

import io.lettuce.core.{KeyValue, Limit, ScoredValue, ScoredValueScanCursor}
import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import monix.reactive.Observable

class RedisSortedSetSpec
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

  s"${RedisSortedSet} " should "implement the RedisSortedSet trait" in {
    RedisSortedSet shouldBe a[RedisSortedSet]
  }

  it should "implement bzpopmin" in {
    //given
    val timeout: Long = genLong.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.bzpopmin(timeout, keys: _*)).thenReturn(MockRedisFuture[KeyValue[K, ScoredValue[V]]])

    //when
    val t = RedisSortedSet.bzpopmin(timeout, keys: _*)

    //then
    t shouldBe a[Task[KeyValue[K, ScoredValue[V]]]]
    verify(asyncRedisCommands).bzpopmin(timeout, keys: _*)
  }

  it should "implement bzpopmax" in {
    //given
    val timeout: Long = genLong.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.bzpopmax(timeout, keys: _*)).thenReturn(MockRedisFuture[KeyValue[K, ScoredValue[V]]])

    //when
    val t = RedisSortedSet.bzpopmax(timeout, keys: _*)

    //then
    t shouldBe a[Task[KeyValue[K, ScoredValue[V]]]]
    verify(asyncRedisCommands).bzpopmax(timeout, keys: _*)
  }

  it should "implement zadd" in {
    //given
    val key: K = genRedisKey.sample.get
    val scoredValues: List[ScoredValue[V]] = genScoredValues.sample.get
    when(asyncRedisCommands.zadd(key, scoredValues: _*)).thenReturn(MockRedisFuture[java.lang.Long])
    //with zArgs not supported

    //when
    val t = RedisSortedSet.zadd(key, scoredValues: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zadd(key, scoredValues: _*)
  }

  it should "implement zaddincr" in {
    //given
    val key: K = genRedisKey.sample.get
    val score = genDouble.sample.get
    val member = genRedisValue.sample.get
    when(asyncRedisCommands.zaddincr(key, score, member)).thenReturn(MockRedisFuture[java.lang.Double])
    //with zAddArgs not cotemplated

    //when
    val t = RedisSortedSet.zaddincr(key, score, member)

    //then
    t shouldBe a[Task[Double]]
    verify(asyncRedisCommands).zaddincr(key, score, member)
  }

  it should "implement zcard" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.zcard(key)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisSortedSet.zcard(key)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zcard(key)
  }

  it should "implement zcount" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.zcount(key, unboundedRange)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisSortedSet.zcount(key, unboundedRange)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zcount(key, unboundedRange)
  }

  it should "implement zincrby" in {
    //given
    val key: K = genRedisKey.sample.get
    val amount: Double = genDouble.sample.get
    val member: V = genRedisValue.sample.get
    when(asyncRedisCommands.zincrby(key, amount, member)).thenReturn(MockRedisFuture[java.lang.Double])

    //when
    val t = RedisSortedSet.zincrby(key, amount, member)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zincrby(key, amount, member)
  }

  it should "implement zinterstore" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.zinterstore(dest, keys: _*)).thenReturn(MockRedisFuture[java.lang.Long])
    //with zStoreArgs not supported

    //when
    val t = RedisSortedSet.zinterstore(dest, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zinterstore(dest, keys: _*)
  }

  it should "implement zlexcount" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.zlexcount(key, unboundedRange)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisSortedSet.zlexcount(key, unboundedRange)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zlexcount(key, unboundedRange)
  }

  it should "implement zpopmin" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.zpopmin(key)).thenReturn(MockRedisFuture[ScoredValue[V]])

    //when
    val t = RedisSortedSet.zpopmin(key)

    //then
    t shouldBe a[Task[ScoredValue[V]]]
    verify(asyncRedisCommands).zpopmin(key)
  }

  it should "implement zpopmax" in {
    //given
    val key: K = genRedisKey.sample.get
    val count: Long = genLong.sample.get
    when(asyncRedisCommands.zpopmax(key)).thenReturn(MockRedisFuture[ScoredValue[V]])
    when(reactiveRedisCommands.zpopmax(key, count)).thenReturn(MockFlux[ScoredValue[V]])

    //when
    val r1 = RedisSortedSet.zpopmax(key)
    val r2 = RedisSortedSet.zpopmax(key, count)

    //then
    r1 shouldBe a[Task[ScoredValue[V]]]
    r2 shouldBe a[Observable[ScoredValue[V]]]
    verify(asyncRedisCommands).zpopmax(key)
    verify(reactiveRedisCommands).zpopmax(key, count)
  }

  it should "implement zrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zrange(key, start, stop)).thenReturn(MockFlux[V])

    //when
    val t = RedisSortedSet.zrange(key, start, stop)

    //then
    t shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).zrange(key, start, stop)
  }

  it should "implement zrangeWithScores" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zrangeWithScores(key, start, stop)).thenReturn(MockFlux[ScoredValue[V]])

    //when
    val t = RedisSortedSet.zrangeWithScores(key, start, stop)

    //then
    t shouldBe a[Observable[ScoredValue[V]]]
    verify(reactiveRedisCommands).zrangeWithScores(key, start, stop)
  }

  it should "implement zrangebylex" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrangebylex(key, unboundedRange)).thenReturn(MockFlux[V])
    when(reactiveRedisCommands.zrangebylex(key, unboundedRange, limit)).thenReturn(MockFlux[V])

    //when
    val r1 = RedisSortedSet.zrangebylex(key, unboundedRange)
    val r2 = RedisSortedSet.zrangebylex(key, unboundedRange, limit)

    //then
    r1 shouldBe a[Observable[V]]
    r2 shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).zrangebylex(key, unboundedRange)
    verify(reactiveRedisCommands).zrangebylex(key, unboundedRange, limit)
  }

  it should "implement zrangebyscore" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrangebyscore(key, unboundedRange)).thenReturn(MockFlux[V])
    when(reactiveRedisCommands.zrangebyscore(key, unboundedRange, limit)).thenReturn(MockFlux[V])

    //when
    val r1 = RedisSortedSet.zrangebyscore(key, unboundedRange)
    val r2 = RedisSortedSet.zrangebyscore(key, unboundedRange, limit)

    //then
    r1 shouldBe a[Observable[V]]
    r2 shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).zrangebyscore(key, unboundedRange)
    verify(reactiveRedisCommands).zrangebyscore(key, unboundedRange, limit)
  }

  it should "implement zrangebyscoreWithScores" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrangebyscoreWithScores(key, unboundedRange)).thenReturn(MockFlux[ScoredValue[V]])
    when(reactiveRedisCommands.zrangebyscoreWithScores(key, unboundedRange, limit)).thenReturn(MockFlux[ScoredValue[V]])

    //when
    val r1 = RedisSortedSet.zrangebyscoreWithScores(key, unboundedRange)
    val r2 = RedisSortedSet.zrangebyscoreWithScores(key, unboundedRange, limit)

    //then
    r1 shouldBe a[Observable[ScoredValue[V]]]
    r2 shouldBe a[Observable[ScoredValue[V]]]
    verify(reactiveRedisCommands).zrangebyscoreWithScores(key, unboundedRange)
    verify(reactiveRedisCommands).zrangebyscoreWithScores(key, unboundedRange, limit)

  }

  it should "implement zrank" in {
    //given
    val key: K = genRedisKey.sample.get
    val member: V = genRedisValue.sample.get
    when(asyncRedisCommands.zrank(key, member)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisSortedSet.zrank(key, member)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zrank(key, member)
  }

  it should "implement zrem" in {
    //given
    val key: K = genRedisKey.sample.get
    val members: List[V] = genRedisValues.sample.get
    when(asyncRedisCommands.zrem(key, members: _*)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisSortedSet.zrem(key, members: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zrem(key, members: _*)
  }

  it should "implement zremrangebylex" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.zremrangebylex(key, unboundedRange)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val r = RedisSortedSet.zremrangebylex(key, unboundedRange)

    //then
    r shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zremrangebylex(key, unboundedRange)
  }

  it should "implement zremrangebyrank" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(asyncRedisCommands.zremrangebyrank(key, start, stop)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisSortedSet.zremrangebyrank(key, start, stop)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zremrangebyrank(key, start, stop)
  }

  it should "implement zremrangebyscore" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.zremrangebyscore(key, unboundedRange)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val r = RedisSortedSet.zremrangebyscore(key, unboundedRange)

    //then
    r shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zremrangebyscore(key, unboundedRange)
  }

  it should "implement zrevrange" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zrevrange(key, start, stop)).thenReturn(MockFlux[V])

    //when
    val t = RedisSortedSet.zrevrange(key, start, stop)

    //then
    t shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).zrevrange(key, start, stop)
  }

  it should "implement zrevrangeWithScores" in {
    //given
    val key: K = genRedisKey.sample.get
    val start: Long = genLong.sample.get
    val stop: Long = genLong.sample.get
    when(reactiveRedisCommands.zrevrangeWithScores(key, start, stop)).thenReturn(MockFlux[ScoredValue[V]])

    //when
    val t = RedisSortedSet.zrevrangeWithScores(key, start, stop)

    //then
    t shouldBe a[Observable[ScoredValue[V]]]
    verify(reactiveRedisCommands).zrevrangeWithScores(key, start, stop)
  }

  it should "implement zrevrangebylex" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrevrangebylex(key, unboundedRange)).thenReturn(MockFlux[V])
    when(reactiveRedisCommands.zrevrangebylex(key, unboundedRange, limit)).thenReturn(MockFlux[V])

    //when
    val r1 = RedisSortedSet.zrevrangebylex(key, unboundedRange)
    val r2 = RedisSortedSet.zrevrangebylex(key, unboundedRange, limit)

    //then
    r1 shouldBe a[Observable[V]]
    r2 shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).zrevrangebylex(key, unboundedRange)
    verify(reactiveRedisCommands).zrevrangebylex(key, unboundedRange, limit)
  }

  it should "implement zrevrangebyscore" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrevrangebyscore(key, unboundedRange)).thenReturn(MockFlux[V])
    when(reactiveRedisCommands.zrevrangebyscore(key, unboundedRange, limit)).thenReturn(MockFlux[V])
    //with channel in args not supported

    //when
    val r1 = RedisSortedSet.zrevrangebyscore(key, unboundedRange)
    val r2 = RedisSortedSet.zrevrangebyscore(key, unboundedRange, limit)

    //then
    r1 shouldBe a[Observable[V]]
    r2 shouldBe a[Observable[V]]
    verify(reactiveRedisCommands).zrevrangebyscore(key, unboundedRange)
    verify(reactiveRedisCommands).zrevrangebyscore(key, unboundedRange, limit)
  }

  it should "implement zrevrangebyscoreWithScores" in {
    //given
    val key: K = genRedisKey.sample.get
    val limit: Limit = Limit.unlimited()
    when(reactiveRedisCommands.zrevrangebyscoreWithScores(key, unboundedRange)).thenReturn(MockFlux[ScoredValue[V]])
    when(reactiveRedisCommands.zrevrangebyscoreWithScores(key, unboundedRange, limit))
      .thenReturn(MockFlux[ScoredValue[V]])
    //with channel in args not supported

    //when
    val r1 = RedisSortedSet.zrevrangebyscoreWithScores(key, unboundedRange)
    val r2 = RedisSortedSet.zrevrangebyscoreWithScores(key, unboundedRange, limit)

    //then
    r1 shouldBe a[Observable[ScoredValue[V]]]
    r2 shouldBe a[Observable[ScoredValue[V]]]
    verify(reactiveRedisCommands).zrevrangebyscoreWithScores(key, unboundedRange)
    verify(reactiveRedisCommands).zrevrangebyscoreWithScores(key, unboundedRange, limit)
  }

  it should "implement zrevrank" in {
    //given
    val key: K = genRedisKey.sample.get
    val member: V = genRedisValue.sample.get
    when(asyncRedisCommands.zrevrank(key, member)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisSortedSet.zrevrank(key, member)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zrevrank(key, member)
  }

  it should "implement zscan" in {
    //given
    val key: K = genRedisKey.sample.get
    when(asyncRedisCommands.zscan(key)).thenReturn(MockRedisFuture[ScoredValueScanCursor[V]])
    //with scanArgs and scanCursor not supported

    //when
    val t = RedisSortedSet.zscan(key)

    //then
    t shouldBe a[Task[ScoredValueScanCursor[V]]]
    verify(asyncRedisCommands).zscan(key)
  }

  it should "implement zscore" in {
    //given
    val key: K = genRedisKey.sample.get
    val member: V = genRedisValue.sample.get
    when(asyncRedisCommands.zscore(key, member)).thenReturn(MockRedisFuture[java.lang.Double])

    //when
    val t = RedisSortedSet.zscore(key, member)

    //then
    t shouldBe a[Task[Double]]
    verify(asyncRedisCommands).zscore(key, member)
  }

  it should "implement zunionstore" in {
    //given
    val dest: K = genRedisKey.sample.get
    val keys: List[K] = genRedisKeys.sample.get
    when(asyncRedisCommands.zunionstore(dest, keys: _*)).thenReturn(MockRedisFuture[java.lang.Long])
    //with zStoreArgs not supported

    //when
    val t = RedisSortedSet.zunionstore(dest, keys: _*)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).zunionstore(dest, keys: _*)
  }

}
