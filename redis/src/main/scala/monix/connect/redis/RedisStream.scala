/*
 * Copyright (c) 2014-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
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
import io.lettuce.core.{Limit, Range, StreamMessage, Consumer => LConsumer}
import monix.eval.Task
import monix.reactive.Observable
import io.lettuce.core.XReadArgs.StreamOffset

/**
  * The Stream is a new data type introduced recently, wwhich models a log data structure
  * in a more abstract way, like a log file often implemented as a file open in apend only mode,
  * Redis streams are primarily an append only data structure. At least conceptually, because being Redis streams
  * an abstract data type represented in memory, they implement more powerful opperations,
  * to overcome the limits of the log file itself.
  * Check the official documentation to see the available operations at: https://redis.io/commands#stream
  * @see The reference to lettuce api:
  *      [[io.lettuce.core.api.async.RedisStreamAsyncCommands]] and
  *      [[io.lettuce.core.api.reactive.RedisStreamReactiveCommands]]
  *
  */
trait RedisStream {

  /**
    * Acknowledge one or more messages as processed.
    * @return simple-reply the lenght of acknowledged messages.
    */
  def xack[K, V](key: K, group: K, messageIds: String*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] = {
    Task.from(connection.async().xack(key, group, messageIds: _*)).map(_.longValue())
  }

  /**
    * Append a message to the stream key.
    * @return simple-reply the message Id.
    */
  def xadd[K, V](key: K, body: Map[K, V])(implicit connection: StatefulRedisConnection[K, V]): Task[String] = {
    Task.from(connection.async().xadd(key, body))
  } //1/4

  /**
    * Gets ownership of one or multiple messages in the Pending Entries List of a given stream consumer group.
    *
    * @return simple-reply the { @link StreamMessage}
    */
  def xclaim[K, V](key: K, consumer: LConsumer[K], minIdleTime: Long, messageIds: String*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[StreamMessage[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().xclaim(key, consumer, minIdleTime, messageIds: _*))

  /**
    * Removes the specified entries from the stream. Returns the number of items deleted, that may be different from the number
    * of IDs passed in case certain IDs do not exist.
    * @return simple-reply number of removed entries.
    */
  def xdel[K, V](key: K, messageIds: String*)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().xadd(key, messageIds: _*))

  /**
    * Create a consumer group.
    * @return simple-reply { @literal true} if successful.
    */
  def xgroupCreate[K, V](streamOffset: StreamOffset[K], group: K)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().xgroupCreate(streamOffset, group))

  /**
    * Delete a consumer from a consumer group.
    * @return simple-reply { @literal true} if successful.
    */
  def xgroupDelconsumer[K, V](key: K, consumer: LConsumer[K])(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().xgroupDelconsumer(key, consumer)).map(_.booleanValue())

  /**
    * Destroy a consumer group.
    * @return simple-reply { @literal true} if successful.
    */
  def xgroupDestroy[K, V](key: K, group: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Boolean] =
    Task.from(connection.async().xgroupDestroy(key, group)).map(_.booleanValue())

  /**
    * Set the current group id.
    * @return simple-reply OK
    */
  def xgroupSetid[K, V](streamOffset: StreamOffset[K], group: K)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().xgroupSetid(streamOffset, group))
  /**
    * Get the length of a steam.
    * @return simple-reply the lenght of the stream.
    */
  def xlen[K, V](key: K)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().xlen(key)).map(_.longValue())

  /**
    * Read pending messages from a stream for a group.
    * @return List&lt;Object&gt; array-reply list pending entries.
    */
  def xpending[K, V](key: K, group: K)(implicit connection: StatefulRedisConnection[K, V]): Observable[Any] =
    Observable.fromReactivePublisher(connection.reactive().xpending(key, group))

  /**
    * Read pending messages from a stream within a specific [[Range]].
    * @return List&lt;Object&gt; array-reply list with members of the resulting stream.
    */
  def xpending[K, V](key: K, group: K, range: Range[String], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[Any] =
    Observable.fromReactivePublisher(connection.reactive().xpending(key, group))

  /**
    * Read pending messages from a stream within a specific [[Range]].
    * @return List&lt;Object&gt; array-reply list with members of the resulting stream.
    */
  def xpending[K, V](key: K, consumer: LConsumer[K], range: Range[String], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[Any] =
    Observable.fromReactivePublisher(connection.reactive().xpending(key, consumer, range, limit))

  /**
    * Read messages from a stream within a specific [[Range]].
    * @return Members of the resulting stream.
    */
  def xrange[K, V](key: K, range: Range[String])(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[StreamMessage[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().xrange(key, range))

  /**
    * Read messages from a stream within a specific [[Range]] applying a [[Limit]].
    * @return Members of the resulting stream.
    */
  def xrange[K, V](key: K, range: Range[String], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[StreamMessage[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().xrange(key, range))

  /**
    * Read messages from one or more [[StreamOffset]]s.
    * @return Members of the resulting stream.
    */
  def xread[K, V](streams: StreamOffset[K]*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[StreamMessage[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().xread(streams: _*))

  /* /**
   * Read messages from one or more [[StreamOffset]]s.
   */
  def xread[K, V](args: XReadArgs, streams: StreamOffset[K]*)(implicit connection: StatefulRedisConnection[K, V]): Observable[StreamMessage[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().xread(args, streams: _*))*/

  /**
    * Read messages from one or more [[StreamOffset]]s using a consumer group.
    * @return List&lt;StreamMessage&gt; array-reply list with members of the resulting stream.
    */
  def xreadgroup[K, V](consumer: LConsumer[K], streams: StreamOffset[K]*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[StreamMessage[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().xreadgroup(consumer, streams: _*))

  /**
    * Read messages from a stream within a specific [[Range]] in reverse order.
    * @return Members of the resulting stream.
    */
  def xrevrange[K, V](key: K, range: Range[String])(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[StreamMessage[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().xrevrange(key, range))

  /**
    * Read messages from a stream within a specific [[Range]] applying a [[Limit]] in reverse order.
    * @return Meembers of the resulting stream.
    */
  def xrevrange[K, V](key: K, range: Range[String], limit: Limit)(
    implicit
    connection: StatefulRedisConnection[K, V]): Observable[StreamMessage[K, V]] =
    Observable.fromReactivePublisher(connection.reactive().xrevrange(key, range))

  /**
    * Trims the stream to count elements.
    * @return Number of removed entries.
    */
  def xtrim[K, V](key: K, count: Long)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().xtrim(key, count)).map(_.longValue())

  /**
    * Trims the stream to count elements.
    * @return Number of removed entries.
    */
  def xtrim[K, V](key: K, approximateTrimming: Boolean, count: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().xtrim(key, approximateTrimming, count)).map(_.longValue())

}

/**
  * Exposes only methods from the RedisStream api
  */
object RedisStream extends RedisStream
