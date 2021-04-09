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

import io.lettuce.core.api.reactive.RedisKeyReactiveCommands
import monix.eval.Task
import monix.reactive.Observable

import java.util.Date
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.util.Try

/**
  * Exposes the set of redis key commands available.
  *
  * @see <a href="https://redis.io/commands#generic">Key commands reference</a>.
  */
final class KeyCommands[K, V] private[redis] (reactiveCmd: RedisKeyReactiveCommands[K, V]) {

  /**
    * Delete one or more keys.
    *
    * @see <a href="https://redis.io/commands/del">DEL</a>.
    * @return The number of keys that were removed.
    */
  def del(keys: K*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.del(keys: _*))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Return a serialized version of the value stored at the specified key.
    *
    * @see <a href="https://redis.io/commands/dump">DUMP</a>.
    * @return The serialized value.
    */
  def dump(key: K): Task[Array[Byte]] =
    Task
      .fromReactivePublisher(reactiveCmd.dump(key))
      .map(_.getOrElse(Array.emptyByteArray))

  /**
    * Determine how many keys exist.
    *
    * @see <a href="https://redis.io/commands/exists">EXISTS</a>.
    * @return Number of existing keys
    */
  def exists(keys: List[K]): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.exists(keys: _*))
      .map(_.map(_.longValue).getOrElse(0L))

  def exists(key: K): Task[Boolean] =
    Task
      .fromReactivePublisher(reactiveCmd.exists(key))
      .map(_.exists(_.longValue >= 1))
  /**
    * Set a key's time to live with a precision of milliseconds.
    *
    * @see <a href="https://redis.io/commands/expire">EXPIRE</a>.
    * @return `true` if the timeout was set.
    *         `false` if key does not exist or the timeout could not be set.
    *
    */
  def expire(key: K, timeout: FiniteDuration): Task[Boolean] =
    Task
      .fromReactivePublisher(reactiveCmd.expire(key, timeout.toSeconds))
      .map(_.exists(_.booleanValue))

  def expireAt(key: K, date: Date): Task[Boolean] =
    Task
      .fromReactivePublisher(reactiveCmd.expireat(key, date))
      .map(_.exists(_.booleanValue))

  /**
    * Find all keys matching the given pattern.
    *
    * @see <a href="https://redis.io/commands/keys">KEYS</a>.
    * @return Keys matching the pattern.
    */
  def keys(pattern: K): Observable[K] =
    Observable.fromReactivePublisher(reactiveCmd.keys(pattern))

  /**
    * Move a key to another database.
    *
    * @see <a href="https://redis.io/commands/move">MOVE</a>.
    * @return True if the move operation succeeded, false if not.
    */
  def move(key: K, db: Int): Task[Boolean] =
    Task.fromReactivePublisher(reactiveCmd.move(key, db)).map(_.exists(_.booleanValue))

  /**
    * Shows the kind of internal representation used in order
    * to store the value associated with a key.
    *
    * @return if exists, returns a [[String]] representing the object encoding.
    */
  def objectEncoding(key: K): Task[Option[String]] =
    Task.fromReactivePublisher(reactiveCmd.objectEncoding(key))

  /**
    * Time since the object stored at the specified key
    * is idle (not requested by read or write operations).
    *
    */
  def objectIdleTime(key: K): Task[Option[FiniteDuration]] =
    Task
      .fromReactivePublisher(reactiveCmd.objectIdletime(key))
      .map(_.map(seconds => Duration(seconds, TimeUnit.SECONDS)))

  /** todo test
    * Number of references of the value associated with the specified key.
    **
    */
  def objectRefCount(key: K): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.objectRefcount(key))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Removes the expiration from a key.
    *
    * @see <a href="https://redis.io/commands/persist">PERSIST</a>.
    * @return `true` if the timeout was removed.
    *         `false` if key does not exist or does not have an associated timeout.
    */
  def persist(key: K): Task[Boolean] =
    Task
      .fromReactivePublisher(reactiveCmd.persist(key))
      .map(_.exists(_.booleanValue))

  /**
    * Return a random key from the keyspace.
    *
    * @see <a href="https://redis.io/commands/randomkey">RANDOMKEY</a>.
    * @return The random key, or null when the database is empty.
    */
  def randomKey(): Task[Option[K]] =
    Task.fromReactivePublisher(reactiveCmd.randomkey())

  /**
    * Rename a key.
    *
    * @see <a href="https://redis.io/commands/rename">RENAME</a>.
    */
  def rename(key: K, newKey: K): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.rename(key, newKey)).void

  /**
    * Rename a key, only if the new key does not exist.
    *
    * @see <a href="https://redis.io/commands/renamenx">RENAMENX</a>.
    * @return `true` if key was renamed to newkey.
    *         `false` if newkey already exists.
    */
  def renameNx(key: K, newKey: K): Task[Boolean] =
    Task
      .fromReactivePublisher(reactiveCmd.renamenx(key, newKey))
      .map(_.exists(_.booleanValue))

  /**
    * Create a key using the provided serialized value, previously obtained using DUMP.
    *
    * @see <a href="https://redis.io/commands/restore">RESTORE</a>.
    */
  def restore(key: K, ttl: FiniteDuration, value: Array[Byte]): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.restore(key, ttl.toMillis, value)).void

  /**
    * Sort the elements in a list, set or sorted set.
    *
    * @see <a href="https://redis.io/commands/sort">SORT</a>.
    * @return Sorted elements.
    */
  def sort(key: K): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.sort(key))

  /**
    * Touch one or more keys. Touch sets the last accessed time for a key. Non-exsitent keys wont get created.
    *
    * @see <a href="https://redis.io/commands/touch">TOUCH</a>.
    * @return The number of found keys.
    */
  def touch(keys: K*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.touch(keys: _*))
      .map(_.map(_.longValue).getOrElse(0))

  /** Get the time to live for a key.
    *
    * @see <a href="https://redis.io/commands/pttl">PTTL</a>.
    * @return It is a variant of the Redis PTTL, but instead of an integer
    *         representing milliseconds, it returns a[[FiniteDuration]].
    *         A negative value `-2` if the key does not exists, or on unexpected error.
    */
  def pttl(key: K): Task[FiniteDuration] =
    Task
      .fromReactivePublisher(reactiveCmd.pttl(key))
      .map(_.flatMap(millis => Try(Duration(millis, TimeUnit.MILLISECONDS)).toOption)
        .getOrElse(Duration(-2, MILLISECONDS)))

  /**
    * Determine the type stored at key.
    *
    * @return Type of key, or none when key does not exist.
    */
  def keyType(key: K): Task[Option[String]] =
    Task.fromReactivePublisher(reactiveCmd.`type`(key))

  /**
    * Unlink one or more keys (non blocking DEL).
    *
    * @see <a href="https://redis.io/commands/unlink">UNLINK</a>.
    * @return The number of keys that were removed.
    */
  def unLink(keys: K*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.unlink(keys: _*))
      .map(_.map(_.longValue).getOrElse(0L))

}

private[redis] object KeyCommands {

  def apply[K, V](reactiveCmd: RedisKeyReactiveCommands[K, V]): KeyCommands[K, V] = {
    new KeyCommands[K, V](reactiveCmd)
  }

}
