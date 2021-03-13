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
import io.lettuce.core.{KeyValue, MapScanCursor, ScanCursor}
import monix.eval.Task
import monix.reactive.Observable

import scala.jdk.CollectionConverters._

private[redis] trait RedisHash {

  /**
    * Delete one or more hash fields.
    * @return Number of fields that were removed from the hash, not including specified but non existing
    *         fields.
    */
  def hdel[K, V](key: K, fields: K*)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().hdel(key, fields: _*)).map(_.longValue)

  /**
    * Determine if a hash field exists.
    * @return True if the hash contains the field.
    *         False if the hash does not contain the field, or key does not exist.                                                                                                                  .
    */
  def hexists[K, V](key: K, field: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().hexists(key, field)).map(_.booleanValue)

  /**
    * Get the value of a hash field.
    * @return The value associated with field, or null when field is not present in the hash or key does not exist.
    *         A failed task with [[NoSuchElementException]] will be returned when the underlying api
    *         returns an empty publisher. i.e: when the key did not exist.
    */
  def hget[K, V](key: K, field: K)(implicit connection: StatefulRedisConnection[K, V]): Task[V] = {
    Task.from(connection.reactive().hget(key, field))
  }

  /**
    * Increment the integer value of a hash field by the given number.
    * @return The value at field after the increment operation.
    */
  def hincrby[K, V](key: K, field: K, amount: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().hincrby(key, field, amount)).map(_.longValue)

  /**
    * Increment the float value of a hash field by the given amount.
    * @return The value of field after the increment.
    */
  def hincrbyfloat[K, V](key: K, field: K, amount: Double)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Double] =
    Task.from(connection.reactive().hincrbyfloat(key, field, amount)).map(_.doubleValue)

  /**
    * Get all the fields and values in a hash.
    *
    * @param key the key
    * @return Map of the fields and their values stored in the hash, or an empty list when key does not exist.
    */
  def hgetall[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Map[K, V]] =
    Task.from(connection.reactive().hgetall(key)).map(_.asScala.toMap)

  /**
    * Get all the fields in a hash.
    * @return The fields in the hash.
    */
  def hkeys[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Observable[K] =
    Observable.fromReactivePublisher(connection.reactive().hkeys(key))

  /**
    * Get the number of fields in a hash.
    * @return Number of fields in the hash, or 0 when key does not exist.
    */
  def hlen[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().hlen(key)).map(_.longValue)

  /**
    * Get the values of all the given hash fields.
    * @return Values associated with the given fields.
    */
  def hmget[K, V](key: K, fields: K*)(implicit connection: StatefulRedisConnection[K, V]): Observable[KeyValue[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().hmget(key, fields: _*))

  /**
    * Set multiple hash fields to multiple values.
    * @return Simple string reply.
    */
  def hmset[K, V](key: K, map: Map[K, V])(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().hmset(key, map.asJava))

  /**
    * Incrementally iterate hash fields and associated values.
    * @return Map scan cursor.
    */
  def hscan[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[MapScanCursor[K, V]] =
    Task.from(connection.reactive().hscan(key))

  def hscan[K, V](key: K, scanCursor: ScanCursor)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[MapScanCursor[K, V]] =
    Task.from(connection.reactive().hscan(key, scanCursor))

  /**
    * Set the string value of a hash field.
    * @return True if field is a new field in the hash and value was set.
    *         False if field already exists in the hash and the value was updated.
    */
  def hset[K, V](key: K, field: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().hset(key, field, value)).map(_.booleanValue)

  /**
    * Set the value of a hash field, only if the field does not exist.
    *
    * @return True if field is a new field in the hash and value was set.
    *         False if field already exists in the hash and the value was updated.
    */
  def hsetnx[K, V](key: K, field: K, value: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.reactive().hsetnx(key, field, value)).map(_.booleanValue)

  /**
    * Get the string length of the field value in a hash.
    * @return Length of the field value, or 0 when field is not present in the hash
    *         or key does not exist at all.
    */
  def hstrlen[K, V](key: K, field: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().hstrlen(key, field)).map(_.longValue)

  /**
    * Get all the values in a hash.
    * @return Values in the hash, or an empty list when key does not exist.
    */
  def hvals[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Observable[V] =
    Observable.fromReactivePublisher(connection.reactive().hvals(key))
}

object RedisHash extends RedisHash