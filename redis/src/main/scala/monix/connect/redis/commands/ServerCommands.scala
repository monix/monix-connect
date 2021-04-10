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

import java.time.Instant
import java.util.Date
import scala.jdk.CollectionConverters._

/**
  * Exposes the set of redis **server** commands available.
  * @note Does yet not support `clientCaching`, `clientGetredir`, `commandCount, `clientKill`, `bgRewriteAOF`.
  * @see <a href="https://redis.io/commands#server">Server commands reference</a>.
  */
final class ServerCommands[K, V] private[redis] (reactiveCmd: RedisServerReactiveCommands[K, V]) {

  /**
    * Get the current connection name.
    * @return The connection name, if set.
    */
  def clientName: Task[Option[K]] =
    Task.fromReactivePublisher(reactiveCmd.clientGetname())

  /** Set the current connection name. */
  def clientNameSet(name: K): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.clientSetname(name)).void

  /**
    * Get the list of client connections.
    *
    * @see <a href="https://redis.io/commands/hvals">HVALS</a>.
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
    *
    * @see <a href="https://redis.io/commands/config-get">CONFIG GET</a>.
    * @return Bulk string reply
    */
  def configGet(parameter: String): Task[Option[String]] =
    Task
      .fromReactivePublisher(reactiveCmd.configGet(parameter))
      .map(_.map(_.asScala.toMap).flatMap(_.headOption.map(_._2)))

  /**
    * Set a configuration parameter to the given value.
    * All the supported parameters have the same meaning.
    * of the equivalent configuration parameter used in the
    * https://raw.githubusercontent.com/redis/redis/6.0/redis.conf
    *
    * @see <a href="https://redis.io/commands/config-set">CONFIG SET</a>.
    */
  def configSet(parameter: String, value: String): Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.configSet(parameter, value)).void

  /**
    * Get the number of keys in the selected database.
    *
    * @see <a href="https://redis.io/commands/dbsize">DBSIZE</a>.
    */
  def dbSize: Task[Long] = Task.fromReactivePublisher(reactiveCmd.dbsize()).map(_.map(_.toLong).getOrElse(0L))

  /**
    * Remove all keys from all databases.
    *
    * @see <a href="https://redis.io/commands/flushall">FLUSHALL</a>.
    * @return Simple string reply
    */
  def flushAll: Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.flushallAsync()).void

  /**
    * Remove all keys from the current database.
    *
    * @see <a href="https://redis.io/commands/flushdb">FLUSHDB</a>.
    * @return Single string reply
    */
  def flushDb: Task[Unit] =
    Task.fromReactivePublisher(reactiveCmd.flushdbAsync()).void

  /**
    * Get information and statistics about the server as a collection of text lines.
    *
    * @see <a href="https://redis.io/commands/info">INFO</a>.
    */
  def info: Task[String] = Task.fromReactivePublisher(reactiveCmd.info()).map(_.getOrElse(""))

  /**
    * Get the section information and statistics about
    * the server as a collection of text lines.
    *
    * @see <a href="https://redis.io/commands/info">INFO</a>.
    */
  def info(section: String): Task[String] =
    Task.fromReactivePublisher(reactiveCmd.info(section)).map(_.getOrElse(""))

  /**
    * Get the [[Date]] of the last successful save to disk if any.
    *
    * @see <a href="https://redis.io/commands/lastsave">LASTSAVE</a>.
    */
  def lastSave: Task[Option[Date]] = Task.fromReactivePublisher(reactiveCmd.lastsave())

  /**
    * Number of bytes that a key and its value require to be stored in RAM.
    *
    * @see <a href="https://redis.io/commands/memory-stats">MEMORY STATS</a>.
    */
  def memoryUsage(key: K): Task[Long] =
    Task
      .fromReactivePublisher(reactiveCmd.memoryUsage(key))
      .map(_.map(_.longValue()).getOrElse(0L))

  /**
    * Synchronously save the dataset to disk
    *
    * @see <a href="https://redis.io/commands/save">SAVE</a>.
    */
  def save: Task[Unit] = Task.fromReactivePublisher(reactiveCmd.save()).void

}

object ServerCommands {
  def apply[K, V](reactiveCmd: RedisServerReactiveCommands[K, V]): ServerCommands[K, V] =
    new ServerCommands[K, V](reactiveCmd)
}
