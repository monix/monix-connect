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

import io.lettuce.core.api.reactive.RedisListReactiveCommands
import monix.connect.redis.kvToTuple
import monix.eval.Task
import monix.reactive.Observable

import scala.concurrent.duration.FiniteDuration

/**
  * Exposes the set of redis **list** commands available.
  * @see <a href="https://redis.io/commands#list">List commands reference</a>.
  * No support for `bLPop`, `bRPop`, `bRPopLPush`.
  */
class ListCommands[K, V] private[redis] (reactiveCmd: RedisListReactiveCommands[K, V]) {

  /**
    * Get an element from a list by its index.
    * @return The requested element, or null when index is out of range.
    */
  def lIndex(key: K, index: Long): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.lindex(key, index))

  /**
    * Insert an element before or after another element in a list.
    * @return The length of the list after the insert operation, or -1 when the value pivot was not found.
    */
  def lInsert(key: K, before: Boolean, pivot: V, value: V): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.linsert(key, before, pivot, value)).map(_.map(_.longValue()) getOrElse (-1L))

  /**
    * Insert an element before the pivot element in the list.
    *
    * @return `True` if the element was successfully inserted
    *         `False` if either the key or the pivot element were not found.
    */
  def lInsertBefore(key: K, pivot: V, value: V): Task[Boolean] =
    lInsert(key, before = true, pivot, value).map(_ > 0L)

  /**
    * Insert an element after the pivot element in the list.
    *
    * @return `True` if the element was successfully inserted
    *         `False` if either the key or the pivot element were not found.
    */
  def lInsertAfter(key: K, pivot: V, value: V): Task[Boolean] =
    lInsert(key, before = false, pivot, value).map(_ > 0L)

  /**
    * Get the length of a list.
    * @return Long integer-reply the length of the list at { @code key}.
    */
  def lLen(key: K): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.llen(key)).map(_.map(_.longValue) getOrElse (0L))

  /**
    * Remove and get the first element in a list.
    * @return The value of the first element, or null when key does not exist.
    */
  def lPop(key: K): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.lpop(key))

  /**
    * Prepend one or multiple values to a list.
    * @return The length of the list after the push operations.
    */
  def lPush(key: K, values: V*): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.lpush(key, values: _*)).map(_.map(_.longValue).getOrElse(0L))

  def lPush(key: K, values: List[V]): Task[Long] =
    lPush(key, values: _*)

  /**
    * Prepend values to a list, only if the list exists.
    * @return The length of the list after the push operation.
    */
  def lPushX(key: K, values: V*): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.lpushx(key, values: _*)).map(_.map(_.longValue()).getOrElse(0L))

  def lPushX(key: K, values: List[V]): Task[Long] =
    lPush(key, values: _*)

  /**
    * Get a range of elements from a list.
    * @return List of elements in the specified range.
    */
  def lRange(key: K, start: Long, stop: Long): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.lrange(key, start, stop))

  /**
    * Remove elements from a list.
    * @return The number of removed elements.
    */
  def lRem(key: K, count: Long, value: V): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.lrem(key, count, value)).map(_.map(_.longValue).getOrElse(0L))

  /**
    * Set the value of an element in a list by its index.
    *
    * @return string reply
    */
  def lSet(key: K, index: Long, value: V): Task[Boolean] =
    Task
      .fromReactivePublisher(reactiveCmd.lset(key, index, value))
      .as(true)
      .onErrorHandle(ex => !List("index out of range", "no such key").exists(ex.getMessage.contains(_)))

  /**
    * Trim a list to the specified range.
    * @return Simple string reply
    */
  def lTrim(key: K, start: Long, stop: Long): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.ltrim(key, start, stop)).void

  /**
    * Remove and get the last element in a list.
    * @return The value of the last element, or null when key does not exist.
    */
  def rPop(key: K): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.rpop(key))

  //todo create PR for lettuce
  /**
    * Remove the last element in a list, prepend it to another list and return it.
    * @return The element being popped and pushed.
    */
  def rPopLPush(source: K, destination: K): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.rpoplpush(source, destination))

  /**
    * Append one or multiple values to a list.
    * @return The length of the list after the push operation.
    */
  def rPush(key: K, values: V*): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.rpush(key, values: _*)).map(_.map(_.longValue).getOrElse(0L))

  def rPush(key: K, values: List[V]): Task[Long] =
    rPush(key, values: _*)

  /**
    * Append values to a list, only if the list exists.
    * @return The length of the list after the push operation.
    */
  def rPushX(key: K, values: V*): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.rpushx(key, values: _*)).map(_.map(_.longValue).getOrElse(0L))

  def rPushX(key: K, values: List[V]): Task[Long] = rPushX(key, values: _*)

}

object ListCommands {
  def apply[K, V](reactiveCmd: RedisListReactiveCommands[K, V]): ListCommands[K, V] =
    new ListCommands[K, V](reactiveCmd)
}
