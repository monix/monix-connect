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

import io.lettuce.core.api.reactive.RedisSetReactiveCommands
import monix.eval.Task
import monix.reactive.Observable

/**
  * Exposes the set of redis **set** commands available.
  * @see <a href="https://redis.io/commands#set">Set commands reference</a>.
  */
final class SetCommands[K, V] private[redis] (reactiveCmd: RedisSetReactiveCommands[K, V]) {

  /**
    * Add one or more members to a set.
    * @return The number of elements that were added to the set, not including all the elements already
    *         present into the set.
    */
  def sAdd(key: K, members: V*): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.sadd(key, members: _*)).map(_.map(_.longValue).getOrElse(0L))

  def sAdd(key: K, members: Iterable[V]): Task[Long] = sAdd(key, members.toSeq: _*)

  /**
    * Get the number of members in a set.
    * @return The cardinality (number of elements) of the set, 0 if the key does not exist.
    */
  def sCard(key: K): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.scard(key)).map(_.map(_.longValue).getOrElse(0L))

  /**
    * Subtract the first set with all the successive sets.
    *
    * @return A list with members of the resulting set.
    */
  def sDiff(first: K, rest: K*): Observable[V] = {
    Observable.fromReactivePublisher(reactiveCmd.sdiff((rest.+:(first)): _*))
  }

  def sDiff(first: K, rest: Iterable[K]): Observable[V] = sDiff(first, rest.toSeq: _*)

  /**
    * Subtract multiple sets and store the resulting set in a key.
    * @return The number of elements in the resulting set.
    */
  def sDiffStore(destination: K, first: K, rest: K*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.sdiffstore(destination, (rest.+:(first)): _*))
      .map(_.map(_.longValue).getOrElse(0L))

  def sDiffStore(destination: K, first: K, rest: Iterable[K]): Task[Long] =
    sDiffStore(destination, first, rest.toSeq: _*)

  /**
    * Intersect multiple sets.
    * @return A list with members of the resulting set.
    */
  def sInter(keys: K*): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.sinter(keys: _*))

  def sInter(keys: Iterable[K]): Observable[V] = sInter(keys.toSeq: _*)

  /**
    * Intersect multiple sets and store the resulting set in a key.
    * @return The number of elements in the resulting set.
    */
  def sInterStore(destination: K, keys: K*): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.sinterstore(destination, keys: _*)).map(_.map(_.longValue).getOrElse(0L))

  def sInterStore(destination: K, keys: Iterable[K]): Task[Long] = sInterStore(destination, keys.toSeq: _*)

  /**
    * Determine if a given value is a member of a set.
    * @return True if the element is a member of the set.
    *         False if the element is not a member of the set, or if key does not exist.
    */
  def sIsMember(key: K, member: V): Task[Boolean] =
    Task.fromReactivePublisher(reactiveCmd.sismember(key, member)).map(_.exists(_.booleanValue))

  /**
    * Move a member from one set to another.
    * @return True if the element is moved.
    *         False if the element is not a member of source and no operation was performed.
    */
  def sMove(source: K, destination: K, member: V): Task[Boolean] =
    Task.fromReactivePublisher(reactiveCmd.smove(source, destination, member)).map(_.exists(_.booleanValue))

  /**
    * Get all the members in a set.
    * @return All elements of the set.
    */
  def sMembers(key: K): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.smembers(key))

  /**
    * Remove and return a random member from a set.
    * @return The removed element, or ``None when key does not exist.
    */
  def sPop(key: K): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.spop(key))

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
  def sRandMember(key: K): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.srandmember(key))

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
    Task.fromReactivePublisher(reactiveCmd.srem(key, members: _*)).map(_.map(_.longValue).getOrElse(0L))

  def sRem(key: K, members: Iterable[V]): Task[Long] = sRem(key, members.toSeq: _*)

  /**
    * Add multiple sets.
    * @return The members of the resulting set.
    */
  def sUnion(keys: K*): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.sunion(keys: _*))

  def sUnion(keys: Iterable[K]): Observable[V] = sUnion(keys.toSeq: _*)

  /**
    * Add multiple sets and store the resulting set in a key.
    * @return Long that represents the number of elements in the resulting set.
    */
  def sUnionStore(destination: K, keys: K*): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.sunionstore(destination, keys: _*)).map(_.map(_.longValue).getOrElse(0L))

  def sUnionStore(destination: K, keys: Iterable[K]): Task[Long] = sUnionStore(destination, keys.toSeq: _*)

}

private[redis] object SetCommands {
  def apply[K, V](reactiveCmd: RedisSetReactiveCommands[K, V]): SetCommands[K, V] =
    new SetCommands[K, V](reactiveCmd)

}
