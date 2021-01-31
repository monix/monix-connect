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
import io.lettuce.core.api.async.{RedisKeyAsyncCommands, RedisListAsyncCommands, RedisServerAsyncCommands}
import io.lettuce.core.api.reactive.RedisServerReactiveCommands
import monix.eval.Task

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
  * @see The reference Lettuce Api at:
  *      [[io.lettuce.core.api.reactive.RedisServerReactiveCommands]]
  */
private[redis] class ServerCommands[K, V](reactiveCmd: RedisServerReactiveCommands[K, V]) {

  /**
    * Asynchronously rewrite the append-only file.
    * @return Always OK.
    */
  def bgRewriteAOF: Task[String] =
    Task.from(reactiveCmd.bgrewriteaof())

  /**
    * Asynchronously save the dataset to disk.
    * @note `bg` as it is performed in the background
    * @return Simple string reply
    */
  def bgSave: Task[String] =
    Task.from(reactiveCmd.bgsave())

  /**
    * Get the current connection name.
    * @return The connection name, or a null bulk reply if no name is set.
    */
  def clientGetName: Task[String] =
    Task.from(reactiveCmd.bgsave())

  /**
    * Set the current connection name.
    *
    * @return OK if the connection name was successfully set.
    */
  def clientSetName(name: K): Task[String] =
    Task.from(reactiveCmd.bgsave())

  /**
    * Kill the connection of a client identified by ip:port.
    * @return OK if the connection exists and has been closed.
    */
  def clientKill(addr: String): Task[String] =
    Task.from(reactiveCmd.clientKill(addr))

  /**
    * Get the list of client connections.
    *
    * @return A unique string, formatted as follows: One client connection per line (separated by LF),
    *         each line is composed of a succession of property=value fields separated by a space character.
    */
  def clientList: Task[String] =
    Task.from(reactiveCmd.clientList())

  /**
    * Get total number of Redis commands.
    * @return Number of total commands in this Redis server.
    */
  def commandCount: Task[Long] =
    Task.from(reactiveCmd.commandCount()).map(_.longValue)

  /**
    * Get the value of a configuration parameter.
    * @return Bulk string reply
    */
  def configGet(parameter: String): Task[Map[String, String]] =
    Task.from(reactiveCmd.configGet(parameter)).map(_.asScala.toMap)

  /**
    * Reset the stats returned by INFO.
    * @return Always OK.
    */
  def configResetStat: Task[String] =
    Task.from(reactiveCmd.configResetstat())

  /**
    * Remove all keys from all databases.
    * @return Simple string reply
    */
  def flushAll(): Task[String] =
    Task.from(reactiveCmd.flushallAsync())

  /**
    * Remove all keys from the current database.
    * @return Single string reply
    */
  def flushDb(): Task[String] =
    Task.from(reactiveCmd.flushdbAsync())

}

object ServerCommands {
  def apply[K, V](reactiveCmd: RedisServerReactiveCommands[K, V]): ServerCommands[K, V] =
    new ServerCommands[K, V](reactiveCmd)
}
