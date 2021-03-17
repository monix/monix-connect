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

package monix.connect.redis.commands

import io.lettuce.core.api.reactive.RedisSortedSetReactiveCommands
import io.lettuce.core.{Limit, Range}
import monix.connect.redis.domain.{VScore, ZArgs, ZRange}
import monix.connect.redis.domain.ZArgs.ZArg
import monix.connect.redis.kvToTuple
import monix.eval.Task
import monix.reactive.Observable

import scala.concurrent.duration.FiniteDuration

/**
  * Exposes the set of redis sorted set commands available.
  * @see <a href="https://redis.io/commands#sorted_set">Sorted set commands reference</a>.
  *
  * @note Does not support `zRevRange`, `zRevRangeWithScores`, `zRemRangeByRank`.
  */
class SortedSetCommands[K, V] private[redis] (reactiveCmd: RedisSortedSetReactiveCommands[K, V]) {

  /**
    * Removes and returns a member with the lowest scores in the sorted set stored at one of the keys.
    * @return Multi-bulk containing the name of the key, the score and the popped member.
    */
  def bZPopMin(timeout: FiniteDuration, keys: K*): Task[Option[(K, VScore[V])]] =
    Task
      .fromReactivePublisher(reactiveCmd.bzpopmin(timeout.toSeconds, keys: _*))
      .map(_.map(kvToTuple))
      .map(_.map {
        case (t1, t2) =>
          (
            t1,
            t2.map(VScore.from[V])
              .getOrElse(VScore.empty[V]))
      })

  /**
    * Removes and returns a member with the highest scores in the sorted set stored at one of the keys.
    * @return Multi-bulk containing the name of the key, the score and the popped member.
    */
  def bZPopMax(timeout: FiniteDuration, keys: K*): Task[Option[(K, VScore[V])]] =
    Task
      .fromReactivePublisher(reactiveCmd.bzpopmax(timeout.toSeconds, keys: _*))
      .map(_.map(kvToTuple).map {
        case (t1, t2) =>
          (t1, t2.map(VScore.from).getOrElse(VScore.empty))
      })

  /**
    * Add one or more members to a sorted set, or update its score if it already exists.
    * @return Long integer-reply specifically:
    *         True if the member was added to the sorted sets
    *         False if the member was already present, in which case the score is also been updated.
    */
  def zAdd(key: K, score: Double, member: V): Task[Boolean] =
    Task.fromReactivePublisher(reactiveCmd.zadd(key, score, member)).map(_.exists(_.longValue > 0L))

  /**
    * Add one or more members to a sorted set, or update its score if it already exists.
    * @return Long integer-reply specifically:
    *         The number of elements added to the sorted sets, not including elements already existing for which the score was
    *         updated.
    */
  def zAdd(key: K, scoredValues: VScore[V]*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zadd(key, scoredValues.map(_.toScoredValue): _*))
      .map(_.map(_.longValue).getOrElse(0L))

  def zAdd(key: K, scoredValues: List[VScore[V]]): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zadd(key, scoredValues.map(_.toScoredValue): _*))
      .map(_.map(_.longValue).getOrElse(0L))

  def zAdd(key: K, zArg: ZArg, score: Double, value: V): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zadd(key, ZArgs.parse(zArg), score, value))
      .map(_.map(_.toLong)
        .getOrElse(0L))

  def zAdd(key: K, zArg: ZArg, scoredValues: VScore[V]*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zadd(key, ZArgs.parse(zArg), scoredValues.map(_.toScoredValue): _*))
      .map(_.map(_.toLong).getOrElse(0L))

  def zAdd(key: K, zArg: ZArg, scoredValues: List[VScore[V]]): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zadd(key, ZArgs.parse(zArg), scoredValues.map(_.toScoredValue): _*))
      .map(_.map(_.toLong).getOrElse(0L))

  /**
    * Add one or more members to a sorted set, or update its score if it already exists applying the INCR option. ZADD
    * acts like ZINCRBY.
    *
    * @return The total number of elements changed
    */
  def zAddIncr(key: K, score: Double, member: V): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zaddincr(key, score, member))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Add one or more members to a sorted set,
    * or update its score depending on [[ZArgs]].
    *
    * @return The score number of the updated element.
    */
  def zAddIncr(key: K, zArg: ZArg, score: Double, member: V): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zaddincr(key, ZArgs.parse(zArg), score, member))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Get the number of members in a sorted set.
    *
    * @return the cardinality (number of elements) of the sorted set, or 0 if key does not exist..
    */
  def zCard(key: K): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zcard(key))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Count the number of members in a sorted set within a given [[ZRange]].
    * @return The number of elements in the specified score range.
    */
  def zCount(key: K, range: ZRange[_ <: Number]): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zcount(key, range.underlying))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Increment the score of a member in a sorted set.
    * @return The new score of member.
    */
  def zIncrBy(key: K, amount: Double, member: V): Task[Double] =
    Task
      .fromReactivePublisher(reactiveCmd.zincrby(key, amount, member))
      .map(_.map(_.doubleValue).getOrElse(0L))

  /**
    * Intersect multiple sorted sets and store the resulting sorted set in a new key.
    * @return The number of elements in the resulting sorted set at destination.
    */
  def zInterStore(destination: K, keys: K*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zinterstore(destination, keys: _*))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Count the number of members in a sorted set between a given lexicographical range.
    * @return The number of elements in the specified score range.
    */
  def zLexCount(key: K, range: ZRange[_ <: V]): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zlexcount(key, range.underlying))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Removes and returns the lowest score in the sorted set stored at key.
    * @return Scored value the removed element.
    */
  def zPopMin(key: K): Task[VScore[V]] =
    Task
      .fromReactivePublisher(reactiveCmd.zpopmin(key))
      .map(_.map(VScore.from).getOrElse(VScore.empty))

  /**
    * Removes and returns up to count members with the lowest scores in the sorted set stored at key.
    *  @return Scored values of the popped scores and elements.
    */
  def zPopMin(key: K, count: Long): Observable[VScore[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zpopmin(key, count)).map(VScore.from)

  /**
    * Removes and returns up the highest scores in the sorted set stored at key.
    * @return Scored value of the removed element.
    */
  def zPopMax(key: K): Task[VScore[V]] =
    Task
      .fromReactivePublisher(reactiveCmd.zpopmax(key))
      .map(_.map(VScore.from).getOrElse(VScore.empty))

  /**
    * Removes and returns up to count members with the highest scores in the sorted set stored at key.
    * @return Scored values of popped scores and elements.
    */
  def zPopMax(key: K, count: Long): Observable[VScore[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zpopmax(key, count)).map(VScore.from)

  /**
    * Return a range of members in a sorted set, by lexicographical range.
    * @return Elements in the specified range.
    */
  def zRangeByLex(key: K, range: ZRange[_ <: V]): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebylex(key, range.underlying))

  /**
    * Return a range of members in a sorted set, by lexicographical range.
    * @return Elements in the specified range.
    */
  def zRangeByLex(key: K, range: ZRange[_ <: V], limit: Int, offset: Int = 0): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebylex(key, range.underlying, Limit.create(offset, limit)))

  /**
    * Return a range of members in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zRangeByScore(key: K, range: ZRange[_ <: Number]): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebyscore(key, range.underlying))

  /**
    * Return a range of members in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zRangeByScore(key: K, range: ZRange[_ <: Number], limit: Int, offset: Int = 0): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebyscore(key, range.underlying, Limit.create(offset, limit)))

  /**
    * Return a range of members with score in a sorted set, by score.
    * @return Scored values in the specified score range.
    */
  def zRangeByScoreWithScores(key: K, range: ZRange[_ <: Number]): Observable[VScore[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebyscoreWithScores(key, range.underlying)).map(VScore.from)

  /**
    * Return a range of members with score in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zRangeByScoreWithScores(key: K, range: ZRange[_ <: Number], limit: Int, offset: Int = 0): Observable[VScore[V]] =
    Observable
      .fromReactivePublisher(reactiveCmd.zrangebyscoreWithScores(key, range.underlying, Limit.create(offset, limit)))
      .map(VScore.from)

  /**
    * Determine the index of a member in a sorted set.
    * @return The rank of member. A [[None]] if the member does not exist in the sorted set or key does not exist.
    */
  def zRank(key: K, member: V): Task[Option[Long]] =
    Task
      .fromReactivePublisher(reactiveCmd.zrank(key, member))
      .map(_.map(_.longValue))

  /**
    * Remove one or more members from a sorted set.
    * @return The number of members removed from the sorted set, not including non existing members.
    */
  def zRem(key: K, members: V*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zrem(key, members: _*))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Remove all members in a sorted set between the given lexicographical range.
    * @return The number of elements removed.
    */
  def zRemRangeByLex(key: K, range: ZRange[_ <: V]): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zremrangebylex(key, range.underlying))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Remove all members in a sorted set within the given scores.
    * @return The number of elements removed.
    */
  def zRemRangeByScore(key: K, range: ZRange[_ <: Number]): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.zremrangebyscore(key, range.underlying))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Return a range of members in a sorted set, by lexicographical range ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByLex(key: K, range: ZRange[_ <: V]): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebylex(key, range.underlying))

  /**
    * Return a range of members in a sorted set, by lexicographical range ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByLex(key: K, range: ZRange[_ <: V], limit: Int, offset: Int = 0): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebylex(key, range.underlying, Limit.create(offset, limit)))

  /**
    * Return a range of members in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByScore(key: K, range: ZRange[_ <: Number]): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebyscore(key, range.underlying))

  /**
    * Return a range of members in a sorted set, by score, with scores ordered from high to low.
    * Alias for zRevRangeByScoreWithScores
    * @return Elements in the specified score range.
    */
  def zRevRangeByScore(key: K, range: ZRange[_ <: Number], limit: Int, offset: Int = 0): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebyscore(key, range.underlying, Limit.create(offset, limit)))

  /**
    *
    * Return a range of members with scores in a sorted set, by score, with scores ordered from high to low.
    * Alias to zRevRangeByScoreWithScores
    * @return Elements in the specified score range.
    */
  def zRevRangeByScoreWithScores(key: K, range: ZRange[_ <: Number]): Observable[VScore[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebyscoreWithScores(key, range.underlying)).map(VScore.from)

  /**
    * Return a range of members with scores in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zRevRangeByScoreWithScores(
    key: K,
    range: ZRange[_ <: Number],
    limit: Int,
    offset: Int = 0): Observable[VScore[V]] =
    Observable
      .fromReactivePublisher(reactiveCmd.zrevrangebyscoreWithScores(key, range.underlying, Limit.create(offset, limit)))
      .map(VScore.from)

  /**
    * Determine the index of a member in a sorted set, with scores ordered from high to low.
    * @return The rank of member. [[None]] if member or the key does not exist.
    */
  def zRevRank(key: K, member: V): Task[Option[Long]] = {
    Task
      .fromReactivePublisher(reactiveCmd.zrevrank(key, member))
      .map(_.map(_.longValue))
  }

  /**
    * Return a range of members with scores in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zScanVScores(key: K): Observable[VScore[V]] =
    Observable.fromReactivePublisher(reactiveCmd.zrevrangebyscoreWithScores(key, Range.unbounded())).map(VScore.from)

  /**
    *
    * Return a range of members in a sorted set, by index.
    * @return Elements in the specified range.
    */
  def zScanMembers(key: K): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.zrangebyscore(key, Range.unbounded()))

  /**
    * Get the score associated with the given member in a sorted set.
    *
    * @param key the key.
    * @param member the member type: value.
    * @return Double number representing the score string,
    *         '0.0' if the member was not present.
    */
  def zScore(key: K, member: V): Task[Double] = {
    Task.fromReactivePublisher(reactiveCmd.zscore(key, member)).map(_.map(_.doubleValue()).getOrElse(0.0))
  }

  /**
    * Add multiple sorted sets and store the resulting sorted set in a new key.
    * @return The number of elements in the resulting sorted set at destination.
    */
  def zUnionStore(destination: K, keys: K*): Task[Long] = {
    Task.fromReactivePublisher(reactiveCmd.zunionstore(destination, keys: _*)).map(_.map(_.longValue).getOrElse(0L))
  }

}

object SortedSetCommands {
  def apply[K, V](reactiveCmd: RedisSortedSetReactiveCommands[K, V]): SortedSetCommands[K, V] =
    new SortedSetCommands[K, V](reactiveCmd)
}
