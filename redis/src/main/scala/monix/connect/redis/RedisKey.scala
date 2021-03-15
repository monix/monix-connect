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

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.{KeyScanCursor, ScanCursor}
import monix.eval.Task
import monix.reactive.Observable

@deprecated("use the pure `monix.connect.redis.client.RedisConnection`", "0.6.0")
private[redis] trait RedisKey {

  /**
    * Delete one or more keys.
    * @return The number of keys that were removed.
    */
  def del[K, V](keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().del(keys: _*)).map(_.longValue)

  /**
    * Unlink one or more keys (non blocking DEL).
    * @return The number of keys that were removed.
    */
  def unlink[K, V](keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().unlink(keys: _*)).map(_.longValue)

  /**
    * Return a serialized version of the value stored at the specified key.
    * @return The serialized value.
    */
  def dump[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Array[Byte]] =
    Task.from(connection.reactive().dump(key))

  /**
    * Determine how many keys exist.
    * @return Number of existing keys
    */
  def exists[K, V](keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().exists(keys: _*)).map(_.toLong)

  /**
    * Set a key's time to live in seconds.
    * @return True if the timeout was set. false if key does not exist or the timeout could not be set.
    *
    */
  def expire[K, V](key: K, seconds: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().expire(key, seconds)).map(_.booleanValue)

  /**
    * Set the expiration for a key as a UNIX timestamp.
    * @return True if the timeout was set. False if key does not exist or the timeout could not be set.
    */
  def expireat[K, V](key: K, timestamp: Date)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().expireat(key, timestamp)).map(_.booleanValue)

  def expireat[K, V](key: K, timestamp: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().expireat(key, timestamp)).map(_.booleanValue)

  /**
    * Find all keys matching the given pattern.
    * @return Keys matching the pattern.
    */
  def keys[K, V](pattern: K)(implicit connection: StatefulRedisConnection[K, V]): Observable[K] =
    Observable.fromReactivePublisher(connection.reactive().keys(pattern))

  /**
    * Atomically transfer a key from a Redis instance to another one.
    * @return The command returns OK on success.
    */
  def migrate[K, V](host: String, port: Int, key: K, db: Int, timeout: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().migrate(host, port, key, db, timeout))

  /**
    * Move a key to another database.
    * @return True if the move operation succeeded, false if not.
    */
  def move[K, V](key: K, db: Int)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().move(key, db)).map(_.booleanValue)

  /**
    * returns the kind of internal representation used in order to store the value associated with a key.
    * @return String
    */
  def objectEncoding[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().objectEncoding(key))

  /**
    * Returns the number of seconds since the object stored at the specified key is idle (not requested by read or write
    * operations).
    * @return Number of seconds since the object stored at the specified key is idle.
    */
  def objectIdletime[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().objectIdletime(key)).map(_.longValue)

  /**
    * Returns the number of references of the value associated with the specified key.
    * @return Long
    */
  def objectRefcount[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().objectRefcount(key)).map(_.longValue)

  /**
    * Remove the expiration from a key.
    * @return True if the timeout was removed. false if key does not exist or does not have an associated timeout.
    */
  def persist[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().persist(key)).map(_.booleanValue)

  /**
    * Set a key's time to live in milliseconds.
    * @return True if the timeout was set. False if key does not exist or the timeout could not be set.
    *
    */
  def pexpire[K, V](key: K, milliseconds: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().pexpire(key, milliseconds)).map(_.booleanValue())

  /**
    * Set the expiration for a key as a UNIX timestamp specified in milliseconds.
    * @return Boolean integer-reply specifically:
    *         { @literal true} if the timeout was set. { @literal false} if { @code key} does not exist or the timeout could not
    *                                                                               be set (see: { @code EXPIRE}).
    */
  def pexpireat[K, V](key: K, timestamp: Date)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().pexpireat(key, timestamp)).map(_.booleanValue())

  def pexpireat[K, V](key: K, timestamp: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().pexpireat(key, timestamp)).map(_.booleanValue())

  /**
    * Get the time to live for a key in milliseconds.
    * @return Long integer-reply TTL in milliseconds, or a negative value in order to signal an error (see the description
    *         above).
    */
  def pttl[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().pttl(key)).map(_.longValue)

  /**
    * Return a random key from the keyspace.
    * @return The random key, or null when the database is empty.
    */
//  def randomkey[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
//    Task.from(connection.reactive().randomkey())

  /**
    * Rename a key.
    * @return String simple-string-reply
    */
  def rename[K, V](key: K, newKey: K)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().rename(key, newKey))

  /**
    * Rename a key, only if the new key does not exist.
    * @return True if key was renamed to newkey.
    *         False if newkey already exists.
    */
  def renamenx[K, V](key: K, newKey: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().renamenx(key, newKey)).map(_.booleanValue)

  /**
    * Create a key using the provided serialized value, previously obtained using DUMP.
    * @return The command returns OK on success.
    */
  def restore[K, V](key: K, ttl: Long, value: Array[Byte])(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().restore(key, ttl, value))

  /**
    * Sort the elements in a list, set or sorted set.
    * @return Sorted elements.
    */
  def sort[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().sort(key))

  /**
    * Touch one or more keys. Touch sets the last accessed time for a key. Non-exsitent keys wont get created.
    * @return The number of found keys.
    */
  def touch[K, V](keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().touch(keys: _*)).map(_.longValue)

  /**
    * Get the time to live for a key.
    * @return TTL in seconds, or a negative value in order to signal an error (see the description above).
    */
  def ttl[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().ttl(key)).map(_.longValue)

  /**
    * Determine the type stored at key.
    * @return Type of key, or none when key does not exist.
    */
  def `type`[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().`type`(key))

  /**
    * Incrementally iterate the keys space.
    * @return Scan cursor.
    */
  def scan[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[KeyScanCursor[K]] =
    Task.from(connection.reactive().scan())

  def scan[K, V](scanCursor: ScanCursor)(implicit connection: StatefulRedisConnection[K, V]): Task[KeyScanCursor[K]] =
    Task.from(connection.reactive().scan(scanCursor))

}

object RedisKey extends RedisKey
