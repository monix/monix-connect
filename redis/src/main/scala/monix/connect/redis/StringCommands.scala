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

import io.lettuce.core.api.reactive.RedisStringReactiveCommands
import monix.eval.Task
import monix.reactive.Observable

import scala.jdk.CollectionConverters._

/**
  * @see The reference to lettuce api [[io.lettuce.core.api.reactive.RedisStringReactiveCommands]]
  */
private[redis] class StringCommands[K, V](reactiveCmd: RedisStringReactiveCommands[K, V]) {

  /**
    * Append a value to a key.
    * @return The length of the string after the append operation.
    */
  def append(key: K, value: V): Task[Long] =
    Task.from(reactiveCmd.append(key, value)).map(_.longValue)

  /**
    * Count set bits in a string.
    * @return The number of bits set to 1.
    */
  def bitCount(key: K): Task[Long] =
    Task.from(reactiveCmd.bitcount(key)).map(_.longValue)

  /**
    * Count set bits in a string.
    * @return The number of bits set to 1.
    */
  def bitCount(key: K, start: Long, end: Long): Task[Long] =
    Task.from(reactiveCmd.bitcount(key, start, end)).map(_.longValue)

  /**
    * Find first bit set or clear in a string.
    * @return The command returns the position of the first bit set to 1 or 0 according to the request.
    */
  def bitPos(key: K, state: Boolean): Task[Long] =
    Task.from(reactiveCmd.bitpos(key, state)).map(_.longValue)

  /**
    * Find first bit set or clear in a string.
    * @return The command returns the position of the first bit set to 1 or 0 according to the request.
    */
  def bitPos(key: K, state: Boolean, start: Long): Task[Long] =
    Task.from(reactiveCmd.bitpos(key, state, start)).map(_.longValue)

  /**
    * Find first bit set or clear in a string.
    * @return The command returns the position of the first bit set to 1 or 0 according to the request.
    */
  def bitPos(key: K, state: Boolean, start: Long, end: Long): Task[Long] =
    Task.from(reactiveCmd.bitpos(key, state, start, end)).map(_.longValue)

  /**
    * Perform bitwise AND between strings.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitOpAnd(destination: K, keys: K*): Task[Long] =
    Task.from(reactiveCmd.bitopAnd(destination, keys: _*)).map(_.longValue)

  /**
    * Perform bitwise NOT between strings.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitOpNot(destination: K, source: K): Task[Long] =
    Task.from(reactiveCmd.bitopNot(destination, source)).map(_.longValue)

  /**
    * Perform bitwise OR between strings.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitOpOr(destination: K, keys: K*): Task[Long] =
    Task.from(reactiveCmd.bitopOr(destination, keys: _*)).map(_.longValue)

  /**
    * Perform bitwise XOR between strings.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitOpXor(destination: K, keys: K*): Task[Long] =
    Task.from(reactiveCmd.bitopXor(destination, keys: _*)).map(_.longValue)

  /**
    * Decrement the integer value of a key by one.
    * @return The value of key after the decrement
    */
  def decr(key: K): Task[Long] =
    Task.from(reactiveCmd.decr(key)).map(_.longValue)

  /**
    * Decrement the integer value of a key by the given number.
    * @return The value of key after the decrement.
    */
  def decrBy(key: K, amount: Long): Task[Long] =
    Task.from(reactiveCmd.decrby(key, amount)).map(_.longValue)

  /**
    * Get the value of a key.
    * @return The value of key, or null when key does not exist.
    */
  def get(key: K): Task[V] =
    Task.from(reactiveCmd.get(key))

  /**
    * Returns the bit value at offset in the string value stored at key.
    * @return The bit value stored at offset.
    */
  def getBit(key: K, offset: Long): Task[Long] =
    Task.from(reactiveCmd.getbit(key, offset)).map(_.longValue)

  /**
    * Get a substring of the string stored at a key.
    * @return Bulk string reply.
    */
  def getRange(key: K, start: Long, end: Long): Task[V] =
    Task.from(reactiveCmd.getrange(key, start, end))

  /**
    * Set the string value of a key and return its old value.
    * @return The old value stored at key, or null when key did not exist.
    */
  def getSet(key: K, value: V): Task[V] =
    Task.from(reactiveCmd.getset(key, value))

  /**
    * Increment the integer value of a key by one.
    * @return The value of key after the increment.
    */
  def incr(key: K): Task[Long] =
    Task.from(reactiveCmd.incr(key)).map(_.longValue)

  /**
    * Increment the integer value of a key by the given amount.
    * @return The value of key after the increment.
    */
  def incrBy(key: K, amount: Long): Task[Long] =
    Task.from(reactiveCmd.incrby(key, amount)).map(_.longValue)

  /**
    * Increment the float value of a key by the given amount.
    * @return Double bulk string reply the value of key after the increment.
    */
  def incrByFloat(key: K, amount: Double): Task[Double] =
    Task.from(reactiveCmd.incrbyfloat(key, amount)).map(_.doubleValue)

  /**
    * Get the values of all the given keys.
    * @return Values at the specified keys.
    */
  def mGet(keys: K*): Observable[(K, Option[V])] =
    Observable.fromReactivePublisher(reactiveCmd.mget(keys: _*)).map(kvToTuple)

  /** todo - return Unit
    * Set multiple keys to multiple values.
    * @return Always OK since MSET can't fail.
    */
  def mSet(map: Map[K, V]): Task[String] =
    Task.from(reactiveCmd.mset(map.asJava))

  /**
    * Set multiple keys to multiple values, only if none of the keys exist.
    * @return True if the all the keys were set.
    *         False if no key was set (at least one key already existed).
    */
  def mSetNx(map: Map[K, V]): Task[Boolean] =
    Task.from(reactiveCmd.msetnx(map.asJava)).map(_.booleanValue)

  /**
    * Set the string value of a key.
    * @return OK if SET was executed correctly.
    */
  def set(key: K, value: V): Task[String] =
    Task.from(reactiveCmd.set(key, value))

  /**
    * Sets or clears the bit at offset in the string value stored at key.
    * @return The original bit value stored at offset.
    */
  def setBit(key: K, offset: Long, value: Int): Task[Long] =
    Task.from(reactiveCmd.setbit(key, offset, value)).map(_.longValue)

  /**
    * Set the value and expiration of a key.
    * @return Simple string reply.
    */
  def setEx(key: K, seconds: Long, value: V): Task[String] =
    Task.from(reactiveCmd.setex(key, seconds, value))

  /**
    * Set the value and expiration in milliseconds of a key.
    * @return String simple-string-reply
    */
  def pSetEx(key: K, milliseconds: Long, value: V): Task[String] =
    Task.from(reactiveCmd.psetex(key, milliseconds, value))

  /**
    * Set the value of a key, only if the key does not exist.
    * @return True if the key was set.
    *         False if the key was not set
    */
  def setNx(key: K, value: V): Task[Boolean] =
    Task.from(reactiveCmd.setnx(key, value)).map(_.booleanValue)

  /**
    * Overwrite part of a string at key starting at the specified offset.
    * @return The length of the string after it was modified by the command.
    */
  def setRange(key: K, offset: Long, value: V): Task[Long] =
    Task.from(reactiveCmd.setrange(key, offset, value)).map(_.longValue)

  /**
    * Get the length of the value stored in a key.
    * @return The length of the string at key, or 0 when key does not exist.
    */
  def strLen(key: K): Task[Long] =
    Task.from(reactiveCmd.strlen(key)).map(_.longValue)

}

object StringCommands {
  def apply[K, V](reactiveCmd: RedisStringReactiveCommands[K, V]): StringCommands[K, V] =
    new StringCommands[K, V](reactiveCmd)
}
