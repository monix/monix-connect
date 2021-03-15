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

import io.lettuce.core.api.reactive.RedisServerReactiveCommands
import monix.eval.Task

import scala.jdk.CollectionConverters._

/**
  * Exposes the set of redis **server** commands available.
  * @see <a href="https://redis.io/commands#server">Server commands reference</a>.
  */
final class ServerCommands[K, V] private[redis] (reactiveCmd: RedisServerReactiveCommands[K, V]) {

  /**
    * Asynchronously rewrite the append-only file.
    * @return Always OK.
    */
  def bgRewriteAOF: Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.bgrewriteaof()).void

  /**
    * Asynchronously save the dataset to disk.
    * @note `bg` as it is performed in the background
    * @return Simple string reply
    */
  def bgSave: Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.bgsave()).void

  /**
    * Get the current connection name.
    * @return The connection name, or a null bulk reply if no name is set.
    */
  def clientGetName: Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.bgsave()).void

  /**
    * Set the current connection name.
    *
    * @return OK if the connection name was successfully set.
    */
  def clientSetName(name: K): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.bgsave()).void

  /**
    * Kill the connection of a client identified by ip:port.
    * @return OK if the connection exists and has been closed.
    */
  def clientKill(addr: String): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.clientKill(addr)).void

  /**
    * Get the list of client connections.
    *
    * @return A unique string, formatted as follows: One client connection per line (separated by LF),
    *         each line is composed of a succession of property=value fields separated by a space character.
    */
  def clientList: Task[Option[String]] =
    Task.fromReactivePublisher(reactiveCmd.clientList())

  /**
    * Get total number of Redis commands.
    * @return Number of total commands in this Redis server,
    *         0 if the underlying response was empty.
    */
  def commandCount: Task[Long] =
    Task.fromReactivePublisher(reactiveCmd.commandCount()).map(_.map(_.longValue).getOrElse(0L))

  /**
    * Get the value of a configuration parameter.
    * @return Bulk string reply
    */
  def configGet(parameter: String): Task[Map[String, String]] =
    Task.fromReactivePublisher(reactiveCmd.configGet(parameter)).map(_.map(_.asScala.toMap).getOrElse(Map.empty))

  /**
    * Reset the stats returned by INFO.
    * @return Always OK.
    */
  def configResetStat: Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.configResetstat()).void

  /**
    * Remove all keys from all databases.
    * @return Simple string reply
    */
  def flushAll(): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.flushallAsync()).void

  /**
    * Remove all keys from the current database.
    * @return Single string reply
    */
  def flushDb(): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.flushdbAsync()).void

}

object ServerCommands {
  def apply[K, V](reactiveCmd: RedisServerReactiveCommands[K, V]): ServerCommands[K, V] =
    new ServerCommands[K, V](reactiveCmd)
}
