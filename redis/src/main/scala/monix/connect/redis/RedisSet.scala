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
import io.lettuce.core.ValueScanCursor
import monix.eval.Task
import monix.reactive.Observable

private[redis] trait RedisSet {

  /**
    * Add one or more members to a set.
    * @return The number of elements that were added to the set, not including all the elements already
    *         present into the set.
    */
  def sadd[K, V](key: K, members: V*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().sadd(key, members: _*)).map(_.longValue)

  /**
    * Get the number of members in a set.
    * @return The cardinality (number of elements) of the set, or { @literal false} if { @code key} does not
    *                                                                                                             exist.
    */
  def scard[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().scard(key)).map(_.longValue)

  /**
    * Subtract multiple sets.
    * @return A list with members of the resulting set.
    */
  def sdiff[K, V](keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().sdiff(keys: _*))

  /**
    * Subtract multiple sets and store the resulting set in a key.
    * @return The number of elements in the resulting set.
    */
  def sdiffstore[K, V](destination: K, keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().sdiffstore(destination, keys: _*)).map(_.longValue)

  /**
    * Intersect multiple sets.
    * @return A list with members of the resulting set.
    */
  def sinter[K, V](keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().sinter(keys: _*))

  /**
    * Intersect multiple sets and store the resulting set in a key.
    * @return The number of elements in the resulting set.
    */
  def sinterstore[K, V](destination: K, keys: K*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[java.lang.Long] =
    Task.from(connection.async().sinterstore(destination, keys: _*))

  /**
    * Determine if a given value is a member of a set.
    * @return True if the element is a member of the set.
    *         False if the element is not a member of the set, or if key does not exist.
    */
  def sismember[K, V](key: K, member: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().sismember(key, member)).map(_.booleanValue)

  /**
    * Move a member from one set to another.
    * @return True if the element is moved.
    *         False if the element is not a member of source and no operation was performed.
    */
  def smove[K, V](source: K, destination: K, member: V)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().smove(source, destination, member)).map(_.booleanValue)

  /**
    * Get all the members in a set.
    * @return All elements of the set.
    */
  def smembers[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().smembers(key))

  /**
    * Remove and return a random member from a set.
    * @return The removed element, or null when key does not exist.
    */
  def spop[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().spop(key))

  /**
    * Remove and return one or multiple random members from a set.
    * @return The removed element, or null when key does not exist.
    */
  def spop[K, V](key: K, count: Long)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().spop(key, count))

  /**
    * Get one random member from a set.
    * @return Without the additional count argument the command returns a Bulk Reply with the
    *         randomly selected element, or null when key does not exist.
    */
  def srandmember[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().srandmember(key))

  /**
    * Get one or multiple random members from a set.
    * @return  The elements without the additional count argument the command returns a Bulk Reply
    * with the randomly selected element, or null when key does not exist.
    */
  def srandmember[K, V](key: K, count: Long)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().srandmember(key, count))

  /**
    * Remove one or more members from a set.
    * @return Long hat represents the number of members that were removed from the set, not including non existing members.
    */
  def srem[K, V](key: K, members: V*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().srem(key, members: _*)).map(_.longValue)

  /**
    * Add multiple sets.
    * @return The members of the resulting set.
    */
  def sunion[K, V](keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().sunion(keys: _*))

  /**
    * Add multiple sets and store the resulting set in a key.
    * @return Long that represents the number of elements in the resulting set.
    */
  def sunionstore[K, V](destination: K, keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().sunionstore(destination, keys: _*)).map(_.longValue)

  /**
    * Incrementally iterate Set elements.
    * @return Scan cursor.
    */
  def sscan[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[ValueScanCursor[V]] =
    Task.from(connection.async().sscan(key))

}

object RedisSet extends RedisSet
