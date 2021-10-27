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

import io.lettuce.core.api.reactive.RedisHashReactiveCommands
import monix.connect.redis.kvToTuple
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.Observable

import scala.jdk.CollectionConverters._

/**
  * Exposes the set of redis hash commands.
  *
  * Does not yet support `incrByFloat`
  * @see <a href="https://redis.io/commands#hash">Hash commands reference</a>.
  */
final class HashCommands[K, V] private[redis] (reactiveCmd: RedisHashReactiveCommands[K, V]) {

  /**
    * Delete one or more hash fields.
    *
    * @see <a href="https://redis.io/commands/hdel">HDEL</a>.
    * @return Number of fields that were removed from the hash, not including specified but non existing
    *         fields.
    */
  def hDel(key: K, fields: List[K]): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.hdel(key, fields: _*)).map(_.map(_.longValue).getOrElse(0L))

  def hDel(key: K, field: K): Task[Boolean] =
    Task.fromReactivePublisher(reactiveCmd.hdel(key, field)).map(_.map(_.longValue).getOrElse(0L) > 0L)

  /**
    * Determine if a hash field exists.
    *
    * @see <a href="https://redis.io/commands/hexists">HEXISTS</a>.
    * @return True if the hash contains the field.
    *         False if the hash does not contain the field, or key does not exist.                                                                                                                  .
    */
  def hExists(key: K, field: K): Task[Boolean] =
    Task.fromReactivePublisher(reactiveCmd.hexists(key, field)).map(_.exists(_.booleanValue))

  /**
    * Get the value of a hash field.
    *
    * @see <a href="https://redis.io/commands/hget">HGET</a>.
    * @return The value associated with field, or null when field is not present in the hash or key does not exist.
    *         A failed task with [[NoSuchElementException]] will be returned when the underlying api
    *         returns an empty publisher. i.e: when the key did not exist.
    */
  def hGet(key: K, field: K): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.hget(key, field))

  /**
    * Increment the integer value of a hash field by the given number.
    *
    * @see <a href="https://redis.io/commands/hincrby">HINCRBY</a>.
    * @return Value of the field after the increment operation.
    */
  def hIncrBy(key: K, field: K, amount: Long): Task[Long] =
    Task.suspend {
      Task
        .fromReactivePublisher(reactiveCmd.hincrby(key, field, amount))
        .map(_.map(_.longValue).getOrElse(0L))
    }

  /**
    * Increment the float value of a hash field by the given amount.
    *
    * @see <a href="https://redis.io/commands/hincrbyfloat">INCRBYFLOAT</a>.
    * @return Value of the field after the increment operation.
    *
    */
  def hIncrBy(key: K, field: K, amount: Double): Task[Double] =
    Task
      .fromReactivePublisher(reactiveCmd.hincrbyfloat(key, field, amount))
      .map(_.map(_.doubleValue).getOrElse(0.0))

  /**
    * Get all the fields and values in a hash.
    *
    * @see <a href="https://redis.io/commands/hdel">HDEL</a>.
    * @param key the key
    * @return Map of the fields and their values stored in the hash, or an empty list when key does not exist.
    */
  def hGetAll(key: K): Observable[(K, V)] =
    Observable
      .fromReactivePublisher(reactiveCmd.hgetall(key))
      .filter(_.hasValue)
      .map(kv => (kv.getKey, kv.getValue))

  /**
    * Get all the fields in a hash.
    *
    * @see <a href="https://redis.io/commands/hkeys">HKEYS</a>.
    * @return The fields in the hash.
    */
  def hKeys(key: K): Observable[K] =
    Observable.fromReactivePublisher(reactiveCmd.hkeys(key))

  /**
    * Get the number of fields in a hash.
    *
    * @see <a href="https://redis.io/commands/hlen">HLEN</a>.
    * @return Number of fields in the hash, or 0 when key does not exist.
    */
  def hLen(key: K): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.hlen(key)).map(_.map(_.longValue).getOrElse(0L))

  /**
    * Get the values of all the given hash fields.
    *
    * @see <a href="https://redis.io/commands/hmget">HMGET</a>.
    * @return Values associated with the given fields.
    */
  def hMGet(key: K, fields: K*): Observable[(K, Option[V])] =
    Observable.fromReactivePublisher(reactiveCmd.hmget(key, fields: _*)).map(kvToTuple)

  /**
    * Set multiple hash fields to multiple values.
    * @see <a href="https://redis.io/commands/hmset">hmset</a>.
    */
  def hMSet(key: K, map: Map[K, V]): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.hmset(key, map.asJava)).void

  /**
    * Set the string value of a hash field.
    *
    * @see <a href="https://redis.io/commands/hset">HSET</a>.
    * @return True if field is a new field in the hash and value was set.
    *         False if field already exists in the hash and the value was updated.
    */
  def hSet(key: K, field: K, value: V): Task[Boolean] =
    Task.fromReactivePublisher(reactiveCmd.hset(key, field, value)).map(_.exists(_.booleanValue))

  /**
    * Set the value of a hash field, only if the field does not exist.
    *
    * @see <a href="https://redis.io/commands/hsetxn">HSETNX</a>.
    * @return True if field is a new field in the hash and value was set.
    *         False if field already exists in the hash and the value was updated.
    */
  def hSetNx(key: K, field: K, value: V): Task[Boolean] =
    Task.fromReactivePublisher(reactiveCmd.hsetnx(key, field, value)).map(_.exists(_.booleanValue))

  /**
    * Get the string length of the field value in a hash.
    *
    * @see <a href="https://redis.io/commands/hstrlen">HSTRLEN</a>.
    * @return Length of the field value, or 0 when field is not present in the hash
    *         or key does not exist at all.
    */
  def hStrLen(key: K, field: K): Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.hstrlen(key, field)).map(_.map(_.longValue).getOrElse(0L))

  /**
    * Get all the values in a hash.
    *
    * @see <a href="https://redis.io/commands/hvals">HVALS</a>.
    * @return Values in the hash, or an empty list when key does not exist.
    */
  def hVals(key: K): Observable[V] = Observable.fromReactivePublisher(reactiveCmd.hvals(key))

}

@InternalApi
private[redis] object HashCommands {
  def apply[K, V](reactiveCmd: RedisHashReactiveCommands[K, V]): HashCommands[K, V] =
    new HashCommands[K, V](reactiveCmd)
}
