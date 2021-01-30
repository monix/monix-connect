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
import io.lettuce.core.ValueScanCursor
import io.lettuce.core.api.async.{RedisKeyAsyncCommands, RedisServerAsyncCommands, RedisSetAsyncCommands}
import io.lettuce.core.api.reactive.{RedisKeyReactiveCommands, RedisServerReactiveCommands, RedisSetReactiveCommands}
import monix.eval.Task
import monix.reactive.Observable

/**
  * @see The reference to lettuce api [[io.lettuce.core.api.reactive.RedisSetReactiveCommands]]
  */
private[redis] trait SetCommands[K, V] {

  protected val asyncCmd: RedisSetAsyncCommands[K, V]
  protected val reactiveCmd: RedisSetReactiveCommands[K, V]

  /**
    * Add one or more members to a set.
    * @return The number of elements that were added to the set, not including all the elements already
    *         present into the set.
    */
  def sAdd(key: K, members: V*): Task[Long] =
    Task.from(asyncCmd.sadd(key, members: _*).toCompletableFuture).map(_.longValue)

  /** todo - false if the key does not exist?
    * Get the number of members in a set.
    * @return The cardinality (number of elements) of the set, or false if the key does not exist.
    */
  def sCard(key: K): Task[Long] =
    Task.from(asyncCmd.scard(key).toCompletableFuture).map(_.longValue)

  /**
    * Subtract multiple sets.
    * @return A list with members of the resulting set.
    */
  def sDiff(keys: K*): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.sdiff(keys: _*))

  /**
    * Subtract multiple sets and store the resulting set in a key.
    * @return The number of elements in the resulting set.
    */
  def sDiffStore(destination: K, keys: K*): Task[Long] =
    Task.from(asyncCmd.sdiffstore(destination, keys: _*).toCompletableFuture).map(_.longValue)

  /**
    * Intersect multiple sets.
    * @return A list with members of the resulting set.
    */
  def sInter(keys: K*): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.sinter(keys: _*))

  /**
    * Intersect multiple sets and store the resulting set in a key.
    * @return The number of elements in the resulting set.
    */
  def sInterStore(destination: K, keys: K*): Task[java.lang.Long] =
    Task.from(asyncCmd.sinterstore(destination, keys: _*).toCompletableFuture)

  /**
    * Determine if a given value is a member of a set.
    * @return True if the element is a member of the set.
    *         False if the element is not a member of the set, or if key does not exist.
    */
  def sIsMember(key: K, member: V): Task[Boolean] =
    Task.from(asyncCmd.sismember(key, member).toCompletableFuture).map(_.booleanValue)

  /**
    * Move a member from one set to another.
    * @return True if the element is moved.
    *         False if the element is not a member of source and no operation was performed.
    */
  def sMove(source: K, destination: K, member: V): Task[Boolean] =
    Task.from(asyncCmd.smove(source, destination, member).toCompletableFuture).map(_.booleanValue)

  /**
    * Get all the members in a set.
    * @return All elements of the set.
    */
  def sMembers(key: K): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.smembers(key))

  /**
    * Remove and return a random member from a set.
    * @return The removed element, or null when key does not exist.
    */
  def sPop(key: K): Task[V] =
    Task.from(asyncCmd.spop(key).toCompletableFuture)

  /**
    * Remove and return one or multiple random members from a set.
    * @return The removed element, or null when key does not exist.
    */
  def sPop(key: K, count: Long): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.spop(key, count))

  /**
    * Get one random member from a set.
    * @return Without the additional count argument the command returns a Bulk Reply with the
    *         randomly selected element, or null when key does not exist.
    */
  def sRandMember(key: K): Task[V] =
    Task.from(asyncCmd.srandmember(key).toCompletableFuture)

  /**
    * Get one or multiple random members from a set.
    * @return  The elements without the additional count argument the command returns a Bulk Reply
    * with the randomly selected element, or null when key does not exist.
    */
  def sRandMember(key: K, count: Long): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.srandmember(key, count))

  /**
    * Remove one or more members from a set.
    * @return Long hat represents the number of members that were removed from the set, not including non existing members.
    */
  def sRem(key: K, members: V*): Task[Long] =
    Task.from(asyncCmd.srem(key, members: _*).toCompletableFuture).map(_.longValue)

  /**
    * Add multiple sets.
    * @return The members of the resulting set.
    */
  def sUnion(keys: K*): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.sunion(keys: _*))

  /**
    * Add multiple sets and store the resulting set in a key.
    * @return Long that represents the number of elements in the resulting set.
    */
  def sUnionStore(destination: K, keys: K*): Task[Long] =
    Task.from(asyncCmd.sunionstore(destination, keys: _*).toCompletableFuture).map(_.longValue)

  /**
    * Incrementally iterate Set elements.
    * @return Scan cursor.
    */
  def sScan(key: K): Task[ValueScanCursor[V]] =
    Task.from(asyncCmd.sscan(key).toCompletableFuture)

}

object SetCommands {
  def apply[K, V](
    asyncCmd: RedisSetAsyncCommands[K, V],
    reactiveCmd: RedisSetReactiveCommands[K, V]): SetCommands[K, V] =
    new SetCommands[K, V] {
      override val asyncCmd: RedisSetAsyncCommands[K, V] = asyncCmd
      override val reactiveCmd: RedisSetReactiveCommands[K, V] = reactiveCmd
    }
}
