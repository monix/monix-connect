/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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
import monix.eval.Task
import monix.reactive.Observable

import collection.JavaConverters._

/**
  * @see The reference Lettuce Api at:
  *      [[io.lettuce.core.api.async.BaseRedisAsyncCommands]] and
  *      [[io.lettuce.core.api.reactive.BaseRedisReactiveCommands]]
  */
trait RedisPubSub {

  /**
    * Post a message to a channel.
    * @return The number of clients that received the message.
    */
  def publish[K, V](channel: K, message: V)(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().publish(channel, message)).map(_.longValue)

  /**
    * Lists the currently *active channels*.
    * @return List of active channels, optionally matching the specified pattern.
    */
  def pubsubChannels[K, V](implicit connection: StatefulRedisConnection[K, V]): Observable[K] =
    Observable.fromReactivePublisher(connection.reactive().pubsubChannels())

  /**
    * Lists the currently *active channels*.
    * @return The list of active channels, optionally matching the specified pattern.
    */
  def pubsubChannels[K, V](channel: K)(implicit connection: StatefulRedisConnection[K, V]): Observable[K] =
    Observable.fromReactivePublisher(connection.reactive().pubsubChannels(channel))

  /**
    * Returns the number of subscribers (not counting clients subscribed to patterns) for the specified channels.
    * @return The list of channels and number of subscribers for every channel.
    *         In this case long remains as [[java.lang.Long]] and not as as [[scala.Long]],
    *         since traversing the map to convert values would imply performance implications
    */
  def pubsubNumsub[K, V](channels: K*)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Map[K, java.lang.Long]] =
    Task.from(connection.async().pubsubNumsub(channels: _*)).map(_.asScala.toMap)

  /**
    * Returns the number of subscriptions to patterns.
    * @return The number of patterns all the clients are subscribed to.
    */
  def pubsubNumpat[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().pubsubNumpat()).map(_.longValue)

  /**
    * Echo the given string.
    * @return Bulk string reply.
    */
  def echo[K, V](msg: V)(implicit connection: StatefulRedisConnection[K, V]): Task[V] =
    Task.from(connection.async().echo(msg))

  /**
    * Return the role of the instance in the context of replication.
    * @return Object array-reply where the first element is one of master, slave, sentinel and the additional
    *         elements are role-specific.
    */
  def role[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[Any] =
    Task.from(connection.async().role())

  /**
    * Ping the server.
    * @return Simple string reply.
    */
  def ping[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().ping())

  /**
    * Switch connection to Read-Only mode when connecting to a cluster.
    * @return Simple string reply.
    */
  def readOnly[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().readOnly())

  /**
    * Switch connection to Read-Write mode (default) when connecting to a cluster.
    * @return Simple string reply.
    */
  def readWrite[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().readWrite())

  /**
    * Instructs Redis to disconnect the connection. Note that if auto-reconnect is enabled then Lettuce will auto-reconnect if
    * the connection was disconnected. Use {@link io.lettuce.core.api.StatefulConnection#close} to close connections and
    * release resources.
    * @return String simple string reply always OK.
    */
  def quit[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.async().quit())

  /**
    * Wait for replication.
    * @return Number of replicas
    */
  def waitForReplication[K, V](replicas: Int, timeout: Long)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.async().waitForReplication(replicas, timeout)).map(_.longValue)

}

object RedisPubSub extends RedisPubSub
