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

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.{KeyValue, Limit, Range, ScoredValue, ScoredValueScanCursor}
import monix.eval.Task
import monix.reactive.Observable

/**
  * @see The reference Lettuce Api at:
  *      [[io.lettuce.core.api.async.RedisSortedSetAsyncCommands]] and
  *      [[io.lettuce.core.api.reactive.RedisSortedSetReactiveCommands]]
  */
private[redis] trait RedisSortedSet {

  /**
    * Removes and returns a member with the lowest scores in the sorted set stored at one of the keys.
    * @return Multi-bulk containing the name of the key, the score and the popped member.
    */
  def bzpopmin[K, V](timeout: Long, keys: K*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[KeyValue[K, ScoredValue[V]]] =
    Task.from(connection.async().bzpopmin(timeout, keys: _*))

  /**
    * Removes and returns a member with the highest scores in the sorted set stored at one of the keys.
    * @return Multi-bulk containing the name of the key, the score and the popped member.
    */
  def bzpopmax[K, V](timeout: Long, keys: K*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[KeyValue[K, ScoredValue[V]]] =
    Task.from(connection.async().bzpopmax(timeout, keys: _*))

  /**
    * Add one or more members to a sorted set, or update its score if it already exists.
    * @return Long integer-reply specifically:
    *         The number of elements added to the sorted sets, not including elements already existing for which the score was
    *         updated.
    */
  def zadd[K, V](key: K, score: Double, member: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zadd(key, score, member)).map(_.longValue)

  /**
    * Add one or more members to a sorted set, or update its score if it already exists.
    * @return Long integer-reply specifically:
    *         The number of elements added to the sorted sets, not including elements already existing for which the score was
    *         updated.
    */
  def zadd[K, V](key: K, scoredValues: ScoredValue[V]*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zadd(key, scoredValues: _*)).map(_.longValue)

  /**
    * Add one or more members to a sorted set, or update its score if it already exists applying the INCR option. ZADD
    * acts like ZINCRBY.
    * @return The total number of elements changed
    */
  def zaddincr[K, V](key: K, score: Double, member: V)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Double] =
    Task.from(connection.async().zaddincr(key, score, member)).map(_.doubleValue)

  /**
    * Get the number of members in a sorted set.
    *
    * @return Long integer-reply specifically:
    *         The number of elements added to the sorted sets, not including elements already existing for which the score was
    *         updated.
    */
  def zcard[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zcard(key)).map(_.longValue)

  /**
    * Count the members in a sorted set with scores within the given [[Range]].
    * @return The number of elements of the sorted set, or false if key does not exist.
    */
  def zcount[K, V](key: K, range: Range[_ <: Number])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zcount(key, range)).map(_.longValue)

  /**
    * Increment the score of a member in a sorted set.
    * @return The new score of member, represented as string.
    */
  def zincrby[K, V](key: K, amount: Double, member: V)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Double] =
    Task.from(connection.async().zincrby(key, amount, member)).map(_.doubleValue)

  /**
    * Intersect multiple sorted sets and store the resulting sorted set in a new key.
    * @return The number of elements in the resulting sorted set at destination.
    */
  def zinterstore[K, V](destination: K, keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zinterstore(destination, keys: _*)).map(_.longValue)

  /**
    * Count the number of members in a sorted set between a given lexicographical range.
    * @return The number of elements in the specified score range.
    */
  def zlexcount[K, V](key: K, range: Range[_ <: V])(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zlexcount(key, range)).map(_.longValue)

  /**
    * Removes and returns up to count members with the lowest scores in the sorted set stored at key.
    * @return Scored value the removed element.
    */
  def zpopmin[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[ScoredValue[V]] =
    Task.from(connection.async().zpopmin(key))

  /**
    * Removes and returns up to count members with the lowest scores in the sorted set stored at key.
    *  @return Scored values of the popped scores and elements.
    */
  def zpopmin[K, V](key: K, count: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(connection.reactive().zpopmin(key, count))

  /**
    * Removes and returns up to count members with the highest scores in the sorted set stored at key.
    * @return Scored value of the removed element.
    */
  def zpopmax[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[ScoredValue[V]] =
    Task.from(connection.async().zpopmax(key))

  /**
    * Removes and returns up to count members with the highest scores in the sorted set stored at key.
    * @return Scored values of popped scores and elements.
    */
  def zpopmax[K, V](key: K, count: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(connection.reactive().zpopmax(key, count))

  /**
    * Return a range of members in a sorted set, by index.
    * @return Elements in the specified range.
    */
  def zrange[K, V](key: K, start: Long, stop: Long)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrange(key, start, stop))

  /**
    * Return a range of members with scores in a sorted set, by index.
    * @return Elements in the specified range.
    */
  def zrangeWithScores[K, V](key: K, start: Long, stop: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(connection.reactive().zrangeWithScores(key, start, stop))

  /**
    * Return a range of members in a sorted set, by lexicographical range.
    * @return Elements in the specified range.
    */
  def zrangebylex[K, V](key: K, range: Range[_ <: V])(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrangebylex(key, range))

  /**
    * Return a range of members in a sorted set, by lexicographical range.
    * @return Elements in the specified range.
    */
  def zrangebylex[K, V](key: K, range: Range[_ <: V], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrangebylex(key, range, limit))

  /**
    * Return a range of members in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zrangebyscore[K, V](key: K, range: Range[_ <: Number])(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrangebyscore(key, range))

  /**
    * Return a range of members in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zrangebyscore[K, V](key: K, range: Range[_ <: Number], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrangebyscore(key, range, limit))

  /**
    * Return a range of members with score in a sorted set, by score.
    * @return Scored values in the specified score range.
    */
  def zrangebyscoreWithScores[K, V](key: K, range: Range[_ <: Number])(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(connection.reactive().zrangebyscoreWithScores(key, range))

  /**
    * Return a range of members with score in a sorted set, by score.
    * @return Elements in the specified score range.
    */
  def zrangebyscoreWithScores[K, V](key: K, range: Range[_ <: Number], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(connection.reactive().zrangebyscoreWithScores(key, range, limit))

  /**
    * Determine the index of a member in a sorted set.
    * @return The rank of member. If member does not exist in the sorted set or key does not exist.
    */
  def zrank[K, V](key: K, member: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zrank(key, member)).map(_.longValue)

  /**
    * Remove one or more members from a sorted set.
    * @return The number of members removed from the sorted set, not including non existing members.
    */
  def zrem[K, V](key: K, members: V*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zrem(key, members: _*)).map(_.longValue)

  /**
    * Remove all members in a sorted set between the given lexicographical range.
    * @return The number of elements removed.
    */
  def zremrangebylex[K, V](key: K, range: Range[_ <: V])(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zremrangebylex(key, range)).map(_.longValue)

  /**
    * Remove all members in a sorted set within the given indexes.
    * @return The number of elements removed.
    */
  def zremrangebyrank[K, V](key: K, start: Long, stop: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zremrangebyrank(key, start, stop)).map(_.longValue)

  /**
    * Remove all members in a sorted set within the given scores.
    *  @return The number of elements removed.
    */
  def zremrangebyscore[K, V](key: K, range: Range[_ <: Number])(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zremrangebyscore(key, range)).map(_.longValue)

  /**
    * Return a range of members in a sorted set, by index, with scores ordered from high to low.
    * @return Elements in the specified range.
    */
  def zrevrange[K, V](key: K, start: Long, stop: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrevrange(key, start, stop))

  /**
    * Return a range of members with scores in a sorted set, by index, with scores ordered from high to low.
    * @return Elements in the specified range.
    */
  def zrevrangeWithScores[K, V](key: K, start: Long, stop: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(connection.reactive().zrevrangeWithScores(key, start, stop))

  /**
    * Return a range of members in a sorted set, by lexicographical range ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zrevrangebylex[K, V](key: K, range: Range[_ <: V])(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrevrangebylex(key, range))

  /**
    * Return a range of members in a sorted set, by lexicographical range ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zrevrangebylex[K, V](key: K, range: Range[_ <: V], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrevrangebylex(key, range, limit))

  /**
    * Return a range of members in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zrevrangebyscore[K, V](key: K, range: Range[_ <: Number])(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrevrangebyscore(key, range))

  /**
    * Return a range of members in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zrevrangebyscore[K, V](key: K, range: Range[_ <: Number], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().zrevrangebyscore(key, range, limit))

  /**
    * Return a range of members with scores in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zrevrangebyscoreWithScores[K, V](key: K, range: Range[_ <: Number])(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(connection.reactive().zrevrangebyscoreWithScores(key, range))

  /**
    * Return a range of members with scores in a sorted set, by score, with scores ordered from high to low.
    * @return Elements in the specified score range.
    */
  def zrevrangebyscoreWithScores[K, V](key: K, range: Range[_ <: Number], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[ScoredValue[V]] =
    Observable.fromReactivePublisher(connection.reactive().zrevrangebyscoreWithScores(key, range, limit))

  /**
    * Determine the index of a member in a sorted set, with scores ordered from high to low.
    * @return The rank of member. If member does not exist in the sorted set or key
    *     does not exist.
    */
  def zrevrank[K, V](key: K, member: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zrevrank(key, member)).map(_.longValue)

  /**
    * Incrementally iterate sorted sets elements and associated scores.
    * @return Scan cursor.
    */
  def zscan[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[ScoredValueScanCursor[V]] =
    Task.from(connection.async().zscan(key))

  /**
    * Get the score associated with the given member in a sorted set.
    * @return The score of member represented as string.
    */
  def zscore[K, V](key: K, member: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Double] =
    Task.from(connection.async().zscore(key, member)).map(_.doubleValue)

  /**
    * Add multiple sorted sets and store the resulting sorted set in a new key.
    * @return The number of elements in the resulting sorted set at destination.
    */
  def zunionstore[K, V](destination: K, keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().zunionstore(destination, keys: _*)).map(_.longValue)

}

object RedisSortedSet extends RedisSortedSet
