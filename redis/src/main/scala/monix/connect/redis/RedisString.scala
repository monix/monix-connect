/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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
import io.lettuce.core._
import monix.eval.Task
import monix.reactive.Observable

import collection.JavaConverters._

/**
  * @see The reference to lettuce api [[io.lettuce.core.api.reactive.RedisStringReactiveCommands]]
  */
private[redis] trait RedisString {

  /**
    * Append a value to a key.
    * @return The length of the string after the append operation.
    */
  def append[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().append(key, value)).map(_.longValue)

  /**
    * Count set bits in a string.
    * @return The number of bits set to 1.
    */
  def bitcount[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitcount(key)).map(_.longValue)

  /**
    * Count set bits in a string.
    * @return The number of bits set to 1.
    */
  def bitcount[K, V](key: K, start: Long, end: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitcount(key, start, end)).map(_.longValue)

  /**
    * Find first bit set or clear in a string.
    * @return The command returns the position of the first bit set to 1 or 0 according to the request.
    */
  def bitpos[K, V](key: K, state: Boolean)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitpos(key, state)).map(_.longValue)

  /**
    * Find first bit set or clear in a string.
    * @return The command returns the position of the first bit set to 1 or 0 according to the request.
    */
  def bitpos[K, V](key: K, state: Boolean, start: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitpos(key, state, start)).map(_.longValue)

  /**
    * Find first bit set or clear in a string.
    * @return The command returns the position of the first bit set to 1 or 0 according to the request.
    */
  def bitpos[K, V](key: K, state: Boolean, start: Long, end: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitpos(key, state, start, end)).map(_.longValue)

  /**
    * Perform bitwise AND between strings.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitopAnd[K, V](destination: K, keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitopAnd(destination, keys: _*)).map(_.longValue)

  /**
    * Perform bitwise NOT between strings.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitopNot[K, V](destination: K, source: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitopNot(destination, source)).map(_.longValue)

  /**
    * Perform bitwise OR between strings.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitopOr[K, V](destination: K, keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitopOr(destination, keys: _*)).map(_.longValue)

  /**
    * Perform bitwise XOR between strings.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitopXor[K, V](destination: K, keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().bitopXor(destination, keys: _*)).map(_.longValue)

  /**
    * Decrement the integer value of a key by one.
    * @return The value of key after the decrement
    */
  def decr[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().decr(key)).map(_.longValue)

  /**
    * Decrement the integer value of a key by the given number.
    * @return The value of key after the decrement.
    */
  def decrby[K, V](key: K, amount: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().decrby(key, amount)).map(_.longValue)

  /**
    * Get the value of a key.
    * @return The value of key, or null when key does not exist.
    */
  def get[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.reactive().get(key))

  /**
    * Returns the bit value at offset in the string value stored at key.
    * @return The bit value stored at offset.
    */
  def getbit[K, V](key: K, offset: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().getbit(key, offset)).map(_.longValue)

  /**
    * Get a substring of the string stored at a key.
    * @return Bulk string reply.
    */
  def getrange[K, V](key: K, start: Long, end: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.reactive().getrange(key, start, end))

  /**
    * Set the string value of a key and return its old value.
    * @return The old value stored at key, or null when key did not exist.
    */
  def getset[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.reactive().getset(key, value))

  /**
    * Increment the integer value of a key by one.
    * @return The value of key after the increment.
    */
  def incr[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().incr(key)).map(_.longValue)

  /**
    * Increment the integer value of a key by the given amount.
    * @return The value of key after the increment.
    */
  def incrby[K, V](key: K, amount: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().incrby(key, amount)).map(_.longValue)

  /**
    * Increment the float value of a key by the given amount.
    * @return Double bulk string reply the value of key after the increment.
    */
  def incrbyfloat[K, V](key: K, amount: Double)(implicit connection: StatefulRedisConnection[K, V]): Task[Double] =
    Task.from(connection.reactive().incrbyfloat(key, amount)).map(_.doubleValue)

  /**
    * Get the values of all the given keys.
    * @return Values at the specified keys.
    */
  def mget[K, V](keys: K*)(implicit connection: StatefulRedisConnection[K, V]): Observable[KeyValue[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().mget(keys: _*))

  /**
    * Set multiple keys to multiple values.
    * @return Always OK since MSET can't fail.
    */
  def mset[K, V](map: Map[K, V])(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().mset(map.asJava))

  /**
    * Set multiple keys to multiple values, only if none of the keys exist.
    * @return True if the all the keys were set.
    *         False if no key was set (at least one key already existed).
    */
  def msetnx[K, V](map: Map[K, V])(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().msetnx(map.asJava)).map(_.booleanValue)

  /**
    * Set the string value of a key.
    * @return OK if SET was executed correctly.
    */
  def set[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().set(key, value))

  /**
    * Sets or clears the bit at offset in the string value stored at key.
    * @return The original bit value stored at offset.
    */
  def setbit[K, V](key: K, offset: Long, value: Int)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().setbit(key, offset, value)).map(_.longValue)

  /**
    * Set the value and expiration of a key.
    * @return Simple string reply.
    */
  def setex[K, V](key: K, seconds: Long, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().setex(key, seconds, value))

  /**
    * Set the value and expiration in milliseconds of a key.
    * @return String simple-string-reply
    */
  def psetex[K, V](key: K, milliseconds: Long, value: V)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().psetex(key, milliseconds, value))

  /**
    * Set the value of a key, only if the key does not exist.
    * @return True if the key was set.
    *         False if the key was not set
    */
  def setnx[K, V](key: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().setnx(key, value)).map(_.booleanValue)

  /**
    * Overwrite part of a string at key starting at the specified offset.
    * @return The length of the string after it was modified by the command.
    */
  def setrange[K, V](key: K, offset: Long, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().setrange(key, offset, value)).map(_.longValue)

  /**
    * Get the length of the value stored in a key.
    * @return The length of the string at key, or 0 when key does not exist.
    */
  def strlen[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().strlen(key)).map(_.longValue)

}

object RedisString extends RedisString
