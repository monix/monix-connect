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

import java.util.Date
import io.lettuce.core.api.async.RedisKeyAsyncCommands
import io.lettuce.core.api.reactive.RedisKeyReactiveCommands
import io.lettuce.core.{KeyScanCursor, ScanCursor}
import monix.eval.Task
import monix.reactive.Observable

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

private[redis] trait KeyCommands[K, V] {

  protected val asyncCmd: RedisKeyAsyncCommands[K, V]
  protected val reactiveCmd: RedisKeyReactiveCommands[K, V]

  /**
    * Delete one or more keys.
    * @return The number of keys that were removed.
    */
  def del(keys: K*): Task[Long] =
    Task.from(asyncCmd.del(keys: _*).toCompletableFuture).map(_.longValue)

  /**
    * Unlink one or more keys (non blocking DEL).
    * @return The number of keys that were removed.
    */
  def unlink(keys: K*): Task[Long] =
    Task.from(asyncCmd.unlink(keys: _*).toCompletableFuture).map(_.longValue)

  /**
    * Return a serialized version of the value stored at the specified key.
    * @return The serialized value.
    */
  def dump(key: K): Task[Array[Byte]] =
    Task.from(asyncCmd.dump(key).toCompletableFuture)

  /**
    * Determine how many keys exist.
    * @return Number of existing keys
    */
  def exists(keys: K*): Task[Long] =
    Task.from(asyncCmd.exists(keys: _*).toCompletableFuture).map(_.toLong)

  /**
    * Set a key's time to live in seconds.
    * @return True if the timeout was set. false if key does not exist or the timeout could not be set.
    *
    */
  def expire(key: K, seconds: Long): Task[Boolean] =
    Task.from(asyncCmd.expire(key, seconds).toCompletableFuture).map(_.booleanValue)

  /**
    * Set the expiration for a key as a UNIX timestamp.
    * @return True if the timeout was set. False if key does not exist or the timeout could not be set.
    */
  def expireAt(key: K, timestamp: Date): Task[Boolean] =
    Task.from(asyncCmd.expireat(key, timestamp).toCompletableFuture).map(_.booleanValue)

  def expireAt(key: K, timestamp: Long): Task[Boolean] =
    Task.from(asyncCmd.expireat(key, timestamp).toCompletableFuture).map(_.booleanValue)

  /**
    * Find all keys matching the given pattern.
    * @return Keys matching the pattern.
    */
  def keys(pattern: K): Observable[K] =
    Observable.fromReactivePublisher(reactiveCmd.keys(pattern))

  /**
    * Atomically transfer a key from a Redis instance to another one.
    * @return The command returns OK on success.
    */
  def migrate(host: String, port: Int, key: K, db: Int, timeout: Long): Task[String] =
    Task.from(asyncCmd.migrate(host, port, key, db, timeout).toCompletableFuture)

  /**
    * Move a key to another database.
    * @return True if the move operation succeeded, false if not.
    */
  def move(key: K, db: Int): Task[Boolean] =
    Task.from(asyncCmd.move(key, db).toCompletableFuture).map(_.booleanValue)

  /**
    * returns the kind of internal representation used in order to store the value associated with a key.
    * @return String
    */
  def objectEncoding(key: K): Task[String] =
    Task.from(asyncCmd.objectEncoding(key).toCompletableFuture)

  /** todo
    * Returns the number of seconds since the object stored at the specified key is idle (not requested by read or write
    * operations).
    * @return Number of seconds since the object stored at the specified key is idle.
    */
  def objectIdleTime(key: K): Task[FiniteDuration] =
    Task.from(asyncCmd.objectIdletime(key).toCompletableFuture).map(seconds => Duration(seconds, TimeUnit.SECONDS))

  /**
    * Returns the number of references of the value associated with the specified key.
    * @return Long
    */
  def objectRefCount(key: K): Task[Long] =
    Task.from(asyncCmd.objectRefcount(key).toCompletableFuture).map(_.longValue)

  /**
    * Remove the expiration from a key.
    * @return True if the timeout was removed. false if key does not exist or does not have an associated timeout.
    */
  def persist(key: K): Task[Boolean] =
    Task.from(asyncCmd.persist(key).toCompletableFuture).map(_.booleanValue)

  /**
    * Set a key's time to live in milliseconds.
    * @return True if the timeout was set. False if key does not exist or the timeout could not be set.
    *
    */
  def pExpire(key: K, milliseconds: Long): Task[Boolean] =
    Task.from(asyncCmd.pexpire(key, milliseconds).toCompletableFuture).map(_.booleanValue())

  /**
    * Set the expiration for a key as a UNIX timestamp specified in milliseconds.
    * @return Boolean integer-reply specifically:
    *         { @literal true} if the timeout was set. { @literal false} if { @code key} does not exist or the timeout could not
    *                                                                               be set (see: { @code EXPIRE}).
    */
  def pExpireAt(key: K, timestamp: Date): Task[Boolean] =
    Task.from(asyncCmd.pexpireat(key, timestamp).toCompletableFuture).map(_.booleanValue())

  def pExpireAt(key: K, timestamp: Long): Task[Boolean] =
    Task.from(asyncCmd.pexpireat(key, timestamp).toCompletableFuture).map(_.booleanValue())

  /** todo return finite duration, duplicated!!
    * Get the time to live for a key in milliseconds.
    * @return Long integer-reply TTL in milliseconds, or a negative value in order to signal an error (see the description
    *         above).
    */
  def pTtl(key: K): Task[Long] =
    Task.from(asyncCmd.pttl(key).toCompletableFuture).map(_.longValue)

  /**
    * Return a random key from the keyspace.
    * @return The random key, or null when the database is empty.
    */
  def randomKey(): Task[V] =
    Task.from(asyncCmd.randomkey().toCompletableFuture)

  /**
    * Rename a key.
    * @return String simple-string-reply
    */
  def rename(key: K, newKey: K): Task[String] =
    Task.from(asyncCmd.rename(key, newKey).toCompletableFuture)

  /**
    * Rename a key, only if the new key does not exist.
    * @return True if key was renamed to newkey.
    *         False if newkey already exists.
    */
  def renameNx(key: K, newKey: K): Task[Boolean] =
    Task.from(asyncCmd.renamenx(key, newKey).toCompletableFuture).map(_.booleanValue)

  /**
    * Create a key using the provided serialized value, previously obtained using DUMP.
    * @return The command returns OK on success.
    */
  def restore(key: K, ttl: Long, value: Array[Byte]): Task[String] =
    Task.from(asyncCmd.restore(key, ttl, value).toCompletableFuture)

  /**
    * Sort the elements in a list, set or sorted set.
    * @return Sorted elements.
    */
  def sort(key: K): Observable[V] =
    Observable.fromReactivePublisher(reactiveCmd.sort(key))

  /**
    * Touch one or more keys. Touch sets the last accessed time for a key. Non-exsitent keys wont get created.
    * @return The number of found keys.
    */
  def touch(keys: K*): Task[Long] =
    Task.from(asyncCmd.touch(keys: _*).toCompletableFuture).map(_.longValue)

  /** todo
    * Get the time to live for a key.
    * @return TTL in seconds, or a negative value in order to signal an error (see the description above).
    */
  def ttl(key: K): Task[Long] =
    Task.from(asyncCmd.ttl(key).toCompletableFuture).map(_.longValue)

  /**
    * Determine the type stored at key.
    * @return Type of key, or none when key does not exist.
    */
  def `type`(key: K): Task[String] =
    Task.from(asyncCmd.`type`(key).toCompletableFuture)

  /** todo!! create an observable that iterates over the scanCursor
    * Incrementally iterate the keys space.
    * @return Scan cursor.
    */
  def scan(): Task[KeyScanCursor[K]] =
    Task.from(asyncCmd.scan().toCompletableFuture).flatMap(s => scan(s))

  def scan(scanCursor: ScanCursor): Task[KeyScanCursor[K]] = {
    Task.from(asyncCmd.scan(scanCursor).toCompletableFuture)
  }

}

private[redis] object KeyCommands {

  def apply[K, V](asyncCmd: RedisKeyAsyncCommands[K, V], reactiveCmd: RedisKeyReactiveCommands[K, V]) =
    new KeyCommands[K, V] {
      override val asyncCmd: RedisKeyAsyncCommands[K, V] = asyncCmd
      override val reactiveCmd: RedisKeyReactiveCommands[K, V] = reactiveCmd
    }

}
