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

package monix.connect.redis.client

import cats.effect.Resource
import io.lettuce.core.AbstractRedisClient
import io.lettuce.core.api.{StatefulConnection, StatefulRedisConnection}
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import monix.connect.redis.commands.{
  HashCommands,
  KeyCommands,
  ListCommands,
  PubSubCommands,
  ServerCommands,
  SetCommands,
  SortedSetCommands,
  StringCommands
}
import monix.eval.Task
import monix.execution.internal.InternalApi

/**
  * Aggregates the different redis commands into a single cmd instance.
  *
  * @see <a href="https://redis.io/commands#hash">Hash</a>,
  *      <a href="https://redis.io/commands#generic">Key</a>,
  *      <a href="https://redis.io/commands#list">List</a>,
  *      <a href="https://redis.io/commands#server">Server</a>,
  *      <a href="https://redis.io/commands#set">Set</a>,
  *      <a href="https://redis.io/commands#sorted_set">SortedSet</a>,
  *      <a href="https://redis.io/commands#string">String</a> commands.
  */
case class RedisCmd[K, V](
  hash: HashCommands[K, V],
  key: KeyCommands[K, V],
  list: ListCommands[K, V],
  server: ServerCommands[K, V],
  set: SetCommands[K, V],
  sortedSet: SortedSetCommands[K, V],
  string: StringCommands[K, V],
  pubSub: PubSubCommands[K, V])

@InternalApi
private[redis] object RedisCmd { self =>

  private[redis] def single[K, V](conn: StatefulRedisPubSubConnection[K, V]): Task[RedisCmd[K, V]] = self.makeCmd(conn)

  private[redis] def cluster[K, V](conn: StatefulRedisClusterPubSubConnection[K, V]): Task[RedisCmd[K, V]] =
    self.makeCmd(conn)

  private[this] def makeCmd[K, V](conn: StatefulConnection[K, V]): Task[RedisCmd[K, V]] = {
    {
      conn match {
        case serverConn: StatefulRedisPubSubConnection[K, V] => Task(serverConn.reactive)
        case serverConn: StatefulRedisClusterPubSubConnection[K, V] => Task(serverConn.reactive)
        case _ => Task.raiseError(new NotImplementedError("Redis configuration yet supported."))
      }
    }.map { cmd =>
      RedisCmd(
        hash = HashCommands(cmd),
        key = KeyCommands(cmd),
        list = ListCommands(cmd),
        server = ServerCommands(cmd),
        set = SetCommands(cmd),
        sortedSet = SortedSetCommands(cmd),
        string = StringCommands(cmd),
        pubSub = PubSubCommands(cmd)
      )
    }
  }

  private[redis] def createResource[K, V, Connection <: StatefulConnection[K, V]](
    acquire: Task[(AbstractRedisClient, Connection)]): Resource[Task, Connection] = {
    Resource
      .make(acquire) {
        case (client, conn) =>
          Task.defer {
            Task
              .from(conn.closeAsync())
              .void
              .guarantee(Task.defer(Task.from(client.shutdownAsync()).void))
          }
      }
      .map(_._2)
  }
}
