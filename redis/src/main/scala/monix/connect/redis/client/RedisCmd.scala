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
import io.lettuce.core.api.{StatefulConnection, StatefulRedisConnection}
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import monix.connect.redis.{
  HashCommands,
  KeyCommands,
  ListCommands,
  ServerCommands,
  SetCommands,
  SortedSetCommands,
  StringCommands
}
import monix.eval.{Coeval, Task}
import monix.execution.internal.InternalApi

case class RedisCmd[K, V](
  hash: HashCommands[K, V],
  key: KeyCommands[K, V],
  list: ListCommands[K, V],
  server: ServerCommands[K, V],
  set: SetCommands[K, V],
  sortedSet: SortedSetCommands[K, V],
  string: StringCommands[K, V])

@InternalApi
private[redis] object RedisCmd { self =>

  private[redis] def single[K, V](conn: StatefulRedisConnection[K, V]): Task[RedisCmd[K, V]] = self.makeCmd(conn)

  private[redis] def cluster[K, V](conn: StatefulRedisClusterConnection[K, V]): Task[RedisCmd[K, V]] =
    self.makeCmd(conn)

  private[this] def makeCmd[K, V](conn: StatefulConnection[K, V]): Task[RedisCmd[K, V]] = {
    {
      conn match {
        case serverConn: StatefulRedisConnection[K, V] => Task(serverConn.reactive)
        case serverConn: StatefulRedisClusterConnection[K, V] => Task(serverConn.reactive)
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
        string = StringCommands(cmd)
      )
    }
  }

  private[redis] def connectResource[K, V, RedisConnection <: StatefulConnection[K, V]](
    connect: Task[RedisConnection]): Resource[Task, RedisConnection] = {
    Resource.make(connect)(connection => Task.evalAsync(connection.close()))
  }
}
