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
import monix.eval.Task

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * @see The reference Lettuce Api at:
  *      [[io.lettuce.core.api.reactive.RedisServerReactiveCommands]]
  */
private[redis] trait RedisServer {

  /**
    * Asynchronously rewrite the append-only file.
    * @return Always OK.
    */
  def bgrewriteaof[K, V](implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().bgrewriteaof())
  /**
    * Asynchronously save the dataset to disk.
    * @return Simple string reply
    */
  def bgsave[K, V](implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().bgsave())

  /**
    * Get the current connection name.
    * @return The connection name, or a null bulk reply if no name is set.
    */
  def clientGetname[K, V](implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().bgsave())

  /**
    * Set the current connection name.
    *
    * @return OK if the connection name was successfully set.
    */
  def clientSetname[K, V](name: K)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().bgsave())

  /**
    * Kill the connection of a client identified by ip:port.
    * @return OK if the connection exists and has been closed.
    */
  def clientKill[K, V](addr: String)(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().clientKill(addr))

  /**
    * Get the list of client connections.
    *
    * @return A unique string, formatted as follows: One client connection per line (separated by LF),
    *         each line is composed of a succession of property=value fields separated by a space character.
    */
  def clientList[K, V](implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().clientList())

  /**
    * Get total number of Redis commands.
    * @return Number of total commands in this Redis server.
    */
  def commandCount[K, V](implicit connection: StatefulRedisConnection[K, V]): Task[Long] =
    Task.from(connection.reactive().commandCount()).map(_.longValue)

  /**
    * Get the value of a configuration parameter.
    * @return Bulk string reply
    */
  def configGet[K, V](parameter: String)(
    implicit
    connection: StatefulRedisConnection[K, V]): Task[mutable.Map[String, String]] =
    Task.from(connection.reactive().configGet(parameter)).map(_.asScala)

  /**
    * Reset the stats returned by INFO.
    * @return Always OK.
    */
  def configResetstat[K, V](implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().configResetstat())

  /**
    * Remove all keys from all databases.
    * @return Simple string reply
    */
  def flushallAsync[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().flushallAsync())

  /**
    * Remove all keys reactivehronously from the current database.
    * @return Single string reply
    */
  def flushdbAsync[K, V]()(implicit connection: StatefulRedisConnection[K, V]): Task[String] =
    Task.from(connection.reactive().flushdbAsync())

}

object RedisServer extends RedisServer
