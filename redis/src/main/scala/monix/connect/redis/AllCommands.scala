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

import io.lettuce.core.api.reactive.{RedisReactiveCommands => RedisReactiveCmd, RedisKeyReactiveCommands => KeyCmd}
import io.lettuce.core.api.{StatefulConnection, StatefulRedisConnection}
import io.lettuce.core.cluster.api.{StatefulRedisClusterConnection => ClusterConnection}
import io.lettuce.core.cluster.api.reactive.{RedisAdvancedClusterReactiveCommands => RedisClusterReactiveCmd}
import monix.connect.redis.AllCommands.CompoundRedisCmd
import monix.eval.Task

/**
  * An object that provides an aggregation of all the different Redis Apis.
  * They can be equally accessed independently or from this object.
  */
class AllCommands[K, V, RCmd <: CompoundRedisCmd[K, V]](private[redis] val connection: StatefulConnection[K, V], reactiveCmd: RCmd) extends KeyCommands(reactiveCmd) {

  private[redis] def close: Task[Unit] = Task.from(connection.closeAsync()).void
}


object AllCommands {

  type CompoundRedisCmd[A, B] = KeyCmd[A, B]

  def apply[K, V, RCmd <: CompoundRedisCmd[K, V]](connection: CompoundRedisCmd[K, V]): AllCommands[K, V, RedisReactiveCmd[K, V]] =
    new AllCommands(connection, connection.reactive)

  def cluster[K, V](connection: ClusterConnection[K, V]): AllCommands[K, V, RedisClusterReactiveCmd[K, V]] =
    new AllCommands(connection, connection.reactive)




}