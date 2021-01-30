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

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.{RedisSetAsyncCommands, RedisSortedSetAsyncCommands}
import io.lettuce.core.api.reactive.{RedisSetReactiveCommands, RedisSortedSetReactiveCommands}
import io.lettuce.core.{KeyValue, Limit, Range, ScoredValue, ScoredValueScanCursor}
import monix.eval.Task
import monix.reactive.Observable

/**
  * @see The reference Lettuce Api at: [[io.lettuce.core.api.reactive.RedisSortedSetReactiveCommands]]
  */
private[redis] trait SortedSetCommands[K, V] {

  protected val asyncCmd: RedisSortedSetAsyncCommands[K, V]
  protected val reactiveCmd: RedisSortedSetReactiveCommands[K, V]

  /**
    * Removes and returns a member with the lowest scores in the sorted set stored at one of the keys.
    * @return Multi-bulk containing the name of the key, the score and the popped member.
    */
  def bZPopMin(timeout: Long, keys: K*): Task[KeyValue[K, ScoredValue[V]]] =
    Task.from(asyncCmd.bzpopmin(timeout, keys: _*).toCompletableFuture)

  /**
    * Removes and returns a member with the highest scores in the sorted set stored at one of the keys.
    * @return Multi-bulk containing the name of the key, the score and the popped member.
    */
  def bZPopMax(timeout: Long, keys: K*): Task[KeyValue[K, ScoredValue[V]]] =
    Task.from(asyncCmd.bzpopmax(timeout, keys: _*).toCompletableFuture)

  /**
    * Add one or more members to a sorted set, or update its score if it already exists.
    * @return Long integer-reply specifically:
    *         The number of elements added to the sorted sets, not including elements already existing for which the score was
    *         updated.
    */
  def zAdd(key: K, score: Double, member: V): Task[Long] =
    Task.from(asyncCmd.zadd(key, score, member).toCompletableFuture).map(_.longValue)

  /**
    * Add one or more members to a sorted set, or update its score if it already exists.
    * @return Long integer-reply specifically:
    *         The number of elements added to the sorted sets, not including elements already existing for which the score was
    *         updated.
    */
  def zAdd(key: K, scoredValues: ScoredValue[V]*): Task[Long] =
    Task.from(asyncCmd.zadd(key, scoredValues: _*).toCompletableFuture).map(_.longValue)

  /**
    * Add one or more members to a sorted set, or update its score if it already exists applying the INCR option. ZADD
    * acts like ZINCRBY.
    * @return The total number of elements changed
    */
  def zAddIncr(key: K, score: Double, member: V): Task[Double] =
    Task.from(asyncCmd.zaddincr(key, score, member).toCompletableFuture).map(_.doubleValue)

  /**
    * Get the number of members in a sorted set.
    *
    * @return Long integer-reply specifically:
    *         The number of elements added to the sorted sets, not including elements already existing for which the score was
    *         updated.
    */
  def zCard(key: K): Task[Long] =
    Task.from(asyncCmd.zcard(key).toCompletableFuture).map(_.longValue)

  /**
    * Count the members in a sorted set with scores within the given [[Range]].
    * @return The number of elements of the sorted set, or false if key does not exist.
    */
  def zCount(key: K, range: Range[_ <: Number]): Task[Long] =
    Task.from(asyncCmd.zcount(key, range).toCompletableFuture).map(_.longValue)

  /**
    * Increment the score of a member in a sorted set.
    * @return The new score of member, represented as string.
    */
  def zIncrBy(key: K, amount: Double, member: V): Task[Double] =
    Task.from(asyncCmd.zincrby(key, amount, member).toCompletableFuture).map(_.doubleValue)

  /**
    * Intersect multiple sorted sets and store the resulting sorted set in a new key.
    * @return The number of elements in the resulting sorted set at destination.
    */
  def zInterStore(destination: K, keys: K*): Task[Long] =
    Task.from(asyncCmd.zinterstore(destination, keys: _*).toCompletableFuture).map(_.longValue)

  /**
    * Count the number of members in a sorted set between a given lexicographical range.
    * @return The number of elements in the specified score range.
    */
  def zLexCount(key: K, range: Range[_ <: V]): Task[Long] =
    Task.from(asyncCmd.zlexcount(key, range).toCompletableFuture).map(_.longValue)

  /**
    * Removes and returns up to count members with the lowest scores in the sorted set stored at key.
    * @return Scored value the removed element.
    */
  def zPopMin(key: K): Task[ScoredValue[V]] =
    Task.from(asyncCmd.zpopmin(key).toCompletableFuture)

  /**
    * Removes and returns up to count members with the lowest scores in the sorted set stored at key.
    *  @return Scored values of the popped scores and elements.
    */
  def zPopMin(key: K, count: Long): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zpopmin(key, count))

  /**
    * Removes and returns up to count members with the highest scores in the sorted set stored at key.
    * @return Scored value of the removed element.
    */
  def zPopMax(key: K): Task[ScoredValue[V]] =
    Task.from(asyncCmd.zpopmax(key).toCompletableFuture)

  /**
    * Removes and returns up to count members with the highest scores in the sorted set stored at key.
    * @return Scored values of popped scores and elements.
    */
  def zPopMax(key: K, count: Long): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zpopmax(key, count))

  /**
    * Return a range of members in a sorted set, by index.
    * @return Elements in the specified range.
    */
  def zRange(key: K, start: Long, stop: Long): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrange(key, start, stop))

  /**
    * Return a range of members with scores in a sorted set, by index.
    * @return Elements in the specified range.
    */
  def zRangeWithScores(key: K, start: Long, stop: Long): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrangeWithScores(key, start, stop))

  /**
    * Return a range of members in a sorted set, by lexicographical range.
    * @return Elements in the specified range.
    */
  def zRangeByLex(key: K, range: Range[_ <: V]): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebylex(key, range))

  /**
    * Return a range of members in a sorted set, by lexicographical range.
    * @return Elements in the specified range.
    */
  def zRangeByLex(key: K, range: Range[_ <: V], limit: Limit): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebylex(key, range, limit))

  /**
    * Return a range of members in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zRangeByScore(key: K, range: Range[_ <: Number]): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebyscore(key, range))

  /**
    * Return a range of members in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zRangeByScore(key: K, range: Range[_ <: Number], limit: Limit): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebyscore(key, range, limit))

  /**
    * Return a range of members with score in a sorted set, by score.
    * @return Scored values in the specified score range.
    */
  def zRangeByScoreWithScores(key: K, range: Range[_ <: Number]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebyscoreWithScores(key, range))

  /**
    * Return a range of members with score in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zRangeByScoreWithScores(key: K, range: Range[_ <: Number], limit: Limit): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebyscoreWithScores(key, range, limit))

  /**
    * Determine the index of a member in a sorted set.
    * @return The rank of member. If member does not exist in the sorted set or key does not exist.
    */
  def zRank(key: K, member: V): Task[Long] =
    Task.from(asyncCmd.zrank(key, member).toCompletableFuture).map(_.longValue)

  /**
    * Remove one or more members from a sorted set.
    * @return The number of members removed from the sorted set, not including non existing members.
    */
  def zRem(key: K, members: V*): Task[Long] =
    Task.from(asyncCmd.zrem(key, members: _*).toCompletableFuture).map(_.longValue)

  /**
    * Remove all members in a sorted set between the given lexicographical range.
    * @return The number of elements removed.
    */
  def zRemRangeByLex(key: K, range: Range[_ <: V]): Task[Long] =
    Task.from(asyncCmd.zremrangebylex(key, range).toCompletableFuture).map(_.longValue)

  /**
    * Remove all members in a sorted set within the given indexes.
    * @return The number of elements removed.
    */
  def zRemRangeByRank(key: K, start: Long, stop: Long): Task[Long] =
    Task.from(asyncCmd.zremrangebyrank(key, start, stop).toCompletableFuture).map(_.longValue)

  /**
    * Remove all members in a sorted set within the given scores.
    *  @return The number of elements removed.
    */
  def zRemRangeByScore(key: K, range: Range[_ <: Number]): Task[Long] =
    Task.from(asyncCmd.zremrangebyscore(key, range).toCompletableFuture).map(_.longValue)

  /**
    * Return a range of members in a sorted set, by index, with scores ordered from high to low.
    * @return Elements in the specified range.
    */
  def zRevRange(key: K, start: Long, stop: Long): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrange(key, start, stop))

  /**
    * Return a range of members with scores in a sorted set, by index, with scores ordered from high to low.
    * @return Elements in the specified range.
    */
  def zRevRangeWithScores(key: K, start: Long, stop: Long): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangeWithScores(key, start, stop))

  /**
    * Return a range of members in a sorted set, by lexicographical range ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByLex(key: K, range: Range[_ <: V]): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebylex(key, range))

  /**
    * Return a range of members in a sorted set, by lexicographical range ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByLex(key: K, range: Range[_ <: V], limit: Limit): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebylex(key, range, limit))

  /**
    * Return a range of members in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByScore(key: K, range: Range[_ <: Number]): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebyscore(key, range))

  /**
    * Return a range of members in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByScore(key: K, range: Range[_ <: Number], limit: Limit): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebyscore(key, range, limit))

  /**
    * Return a range of members with scores in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByScoreWithScores(key: K, range: Range[_ <: Number]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebyscoreWithScores(key, range))

  /**
    * Return a range of members with scores in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByScoreWithScores(key: K, range: Range[_ <: Number], limit: Limit): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebyscoreWithScores(key, range, limit))

  /**
    * Determine the index of a member in a sorted set, with scores ordered from high to low.
    * @return The rank of member. If member does not exist in the sorted set or key
    *     does not exist.
    */
  def zRevRank(key: K, member: V): Task[Long] =
    Task.from(asyncCmd.zrevrank(key, member).toCompletableFuture).map(_.longValue)

  /**
    * Incrementally iterate sorted sets elements and associated scores.
    * @return Scan cursor.
    */
  def zScan(key: K): Task[ScoredValueScanCursor[V]] =
    Task.from(asyncCmd.zscan(key).toCompletableFuture)

  /**
    * Get the score associated with the given member in a sorted set.
    * @return The score of member represented as string.
    */
  def zScore(key: K, member: V): Task[Double] =
    Task.from(asyncCmd.zscore(key, member).toCompletableFuture).map(_.doubleValue)

  /**
    * Add multiple sorted sets and store the resulting sorted set in a new key.
    * @return The number of elements in the resulting sorted set at destination.
    */
  def zUnionStore(destination: K, keys: K*): Task[Long] =
    Task.from(asyncCmd.zunionstore(destination, keys: _*).toCompletableFuture).map(_.longValue)

}

object SortedSetCommands {
  def apply[K, V](
    asyncCmd: RedisSortedSetAsyncCommands[K, V],
    reactiveCmd: RedisSortedSetReactiveCommands[K, V]): SortedSetCommands[K, V] =
    new SortedSetCommands[K, V] {
      override val asyncCmd: RedisSortedSetAsyncCommands[K, V] = asyncCmd
      override val reactiveCmd: RedisSortedSetReactiveCommands[K, V] = reactiveCmd
    }
}
