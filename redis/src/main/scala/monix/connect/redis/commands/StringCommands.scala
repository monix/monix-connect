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

import io.lettuce.core.api.reactive.RedisStringReactiveCommands
import monix.connect.redis.kvToTuple
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.Observable

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * Exposes the set of redis string commands available.
  * @see <a href="https://redis.io/commands#string">String commands reference</a>.
  *
  * @note Does not support `bitfield`.
  */
final class StringCommands[K, V] private[redis] (reactiveCmd: RedisStringReactiveCommands[K, V]) {

  /**
    * Append a value to a key.
    *
    * @see <a href="https://redis.io/commands/append">APPEND</a>.
    * @return The length of the string after the append operation.
    *         0 if the key was empty.
    */
  def append(key: K, value: V): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.append(key, value))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Count set bits in a string.
    *
    * @see <a href="https://redis.io/commands/bitcount">BITCOUNT</a>.
    * @return The number of bits set to 1.
    */
  def bitCount(key: K): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.bitcount(key))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Count set bits in a string.
    *
    * @see <a href="https://redis.io/commands/bitcount">BITCOUNT</a>.
    * @return The number of bits set to 1.
    */
  def bitCount(key: K, start: Long, end: Long): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.bitcount(key, start, end))
      .map(_.map(_.longValue).getOrElse(0L))

  private[this] def bitPos(key: K, state: Boolean): Task[Option[Long]] =
    Task
      .fromReactivePublisher(reactiveCmd.bitpos(key, state))
      .map(_.flatMap(pos => if pos == -1L then Option.empty else Some(pos)))

  /**
    * Find the position of the first bit set in a string.
    *
    * @return The command returns the position of the first bit set to 1.
    *         [[None]] if the key did not exist
    *         If the string is empty or composed of just zero bytes, -1 is returned.
    */
  def bitPosOne(key: K): Task[Option[Long]] =
    bitPos(key, state = true)

  /**
    * Find the position of the first bit clear in a string.
    *
    * @return The command returns the position of the first bit set to 1.
    *         If the key did not exist, was empty or composed
    *         of just zero bytes, [[None]] is returned.
    */
  def bitPosZero(key: K): Task[Option[Long]] =
    bitPos(key, state = false)

  /**
    * Perform bitwise `AND` between strings.
    *
    * @see <a href="https://redis.io/commands/bitopand">BITOPAND</a>.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitOpAnd(destination: K, keys: K*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.bitopAnd(destination, keys: _*))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Perform bitwise NOT between strings.
    *
    * @see <a href="https://redis.io/commands/bitopnot">BITOPNOT</a>.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitOpNot(destination: K, source: K): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.bitopNot(destination, source))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Perform bitwise OR between strings.
    *
    * @see <a href="https://redis.io/commands/bitopor">BITOPOR</a>.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitOpOr(destination: K, keys: K*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.bitopOr(destination, keys: _*))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Perform bitwise XOR between strings.
    *
    * @see <a href="https://redis.io/commands/bitopxor">BITOPXOR</a>.
    * @return The size of the string stored in the destination key, that is equal to the size of the longest
    *         input string.
    */
  def bitOpXor(destination: K, keys: K*): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.bitopXor(destination, keys: _*))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Decrement the integer value of a key by one.
    *
    * @see <a href="https://redis.io/commands/decr">DECR</a>.
    * @return The value of key after the decrement
    */
  def decr(key: K): Task[Option[Long]] =
    Task
      .fromReactivePublisher(reactiveCmd.decr(key))
      .map(_.map(_.longValue))

  /**
    * Decrement the integer value of a key by the given number.
    *
    * @see <a href="https://redis.io/commands/decrby">DECRBY</a>.
    * @return The value of key after the decrement.
    */
  def decrBy(key: K, amount: Long): Task[Option[Long]] =
    Task
      .fromReactivePublisher(reactiveCmd.decrby(key, amount))
      .map(_.map(_.longValue))

  /**
    * Get the value of a key.
    *
    * @see <a href="https://redis.io/commands/get">GET</a>.
    * @return The value of key, or null when key does not exist.
    */
  def get(key: K): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.get(key))

  /**
    * Returns the bit value at offset in the string value stored at key.
    *
    * @see <a href="https://redis.io/commands/getBit">GETBIT</a>.
    * @return The bit value stored at offset.
    */
  def getBit(key: K, offset: Long): Task[Option[Long]] =
    Task
      .fromReactivePublisher(reactiveCmd.getbit(key, offset))
      .map(_.map(_.longValue))

  /**
    * Get a substring of the string stored at a key.
    * Fixme: return None on empty strings
    * @see <a href="https://redis.io/commands/getrange">GETRANGE</a>.
    */
  def getRange(key: K, start: Long, end: Long): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.getrange(key, start, end))

  /**
    * Set the string value of a key and return its old value.
    *
    * @see <a href="https://redis.io/commands/getset">GETSET</a>.
    * @return The old value stored at key, or null when key did not exist.
    */
  def getSet(key: K, value: V): Task[Option[V]] =
    Task.fromReactivePublisher(reactiveCmd.getset(key, value))

  /**
    * Increment the integer value of a key by one.
    *
    * @see <a href="https://redis.io/commands/incr">INCR</a>.
    * @return The value of key after the increment.
    */
  def incr(key: K): Task[Option[Long]] =
    Task
      .fromReactivePublisher(reactiveCmd.incr(key))
      .map(_.map(_.byteValue))

  /**
    * Increment the integer value of a key by the given amount.
    *
    * @see <a href="https://redis.io/commands/incrby">INCRBY</a>.
    * @return The value of key after the increment.
    */
  def incrBy(key: K, amount: Long): Task[Option[Long]] =
    Task
      .fromReactivePublisher(reactiveCmd.incrby(key, amount))
      .map(_.map(_.longValue))

  /**
    * Increment the float value of a key by the given amount.
    * @return Double bulk string reply the value of key after the increment.
    */
  def incrByFloat(key: K, amount: Double): Task[Option[Double]] =
    Task
      .fromReactivePublisher(reactiveCmd.incrbyfloat(key, amount))
      .map(_.map(_.doubleValue))

  /**
    * Get the values of all the given keys.
    *
    * @see <a href="https://redis.io/commands/mget">MGET</a>.
    * @return Values at the specified keys.
    */
  def mGet(keys: K*): Observable[(K, Option[V])] =
    Observable.fromReactivePublisher(reactiveCmd.mget(keys: _*)).map(kvToTuple)

  /**
    * Get the values of all the given keys.
    *
    * @see <a href="https://redis.io/commands/mget">MGET</a>.
    * @return Values at the specified keys.
    */
  def mGet(keys: List[K]): Observable[(K, Option[V])] =
    Observable.fromReactivePublisher(reactiveCmd.mget(keys: _*)).map(kvToTuple)

  /**
    * Set multiple keys to multiple values.
    *
    * @see <a href="https://redis.io/commands/mset">MSET</a>.
    * @return Always [[Unit]] since `MSET` can't fail.
    */
  def mSet(map: Map[K, V]): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.mset(map.asJava)).void

  /**
    * Set multiple keys to multiple values, only if none of the keys exist.
    *
    * @see <a href="https://redis.io/commands/msetnx">MSETNX</a>.
    * @return True if the all the keys were set.
    *         False if a key was not set (at least one key already existed).
    */
  def mSetNx(map: Map[K, V]): Task[Boolean] =
    Task
      .fromReactivePublisher(reactiveCmd.msetnx(map.asJava))
      .map(_.exists(_.booleanValue))

  /**
    * Set the string value of a key.
    * @see <a href="https://redis.io/commands/set">SET</a>.
    */
  def set(key: K, value: V): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.set(key, value)).void

  /**
    * Sets or clears the bit at offset in the string value stored at key.
    * @see <a href="https://redis.io/commands/setbit">SETBIT</a>.
    * @return The original bit value stored at offset.
    */
  def setBit(key: K, offset: Long, value: Int): Task[Option[Long]] =
    Task
      .fromReactivePublisher(reactiveCmd.setbit(key, offset, value))
      .map(_.map(_.longValue))

  /**
    * Set the value and expiration of a key given a timeout
    * in [[FiniteDuration]] (with milliseconds precision).
    *
    * @see <a href="https://redis.io/commands/psetex">PSETEX</a>.
    */
  def setEx(key: K, timeout: FiniteDuration, value: V): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.psetex(key, timeout.toMillis, value)).void

  /**
    * Set the value of a key, only if the key does not exist.
    *
    * @see <a href="https://redis.io/commands/setnx">SETNX</a>.
    * @return `true` if the key was set, otherwise `false`.
    */
  def setNx(key: K, value: V): Task[Boolean] =
    Task
      .fromReactivePublisher(reactiveCmd.setnx(key, value))
      .map(_.exists(_.booleanValue))

  /**
    * Overwrite part of a string at key starting at the specified offset.
    *
    * @see <a href="https://redis.io/commands/setrange">SETRANGE</a>.
    * @return The length of the string after it was modified by the command.
    *
    */
  def setRange(key: K, offset: Long, value: V): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.setrange(key, offset, value))
      .map(_.map(_.longValue).getOrElse(0L))

  /**
    * Get the length of the value stored in a key.
    *
    * @see <a href="https://redis.io/commands/strlen">STRLEN</a>.
    * @return The length of the string at key, or 0 when key does not exist.
    */
  def strLen(key: K): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.strlen(key))
      .map(_.map(_.longValue).getOrElse(0L))

}

@InternalApi
private[redis] object StringCommands {
  def apply[K, V](reactiveCmd: RedisStringReactiveCommands[K, V]): StringCommands[K, V] =
    new StringCommands[K, V](reactiveCmd)
}
