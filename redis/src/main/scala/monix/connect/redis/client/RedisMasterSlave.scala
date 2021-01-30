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

import cats.data.NonEmptyList
import cats.effect.Resource
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.codec.{RedisCodec, Utf8StringCodec}
import io.lettuce.core.masterslave.{MasterSlave, StatefulRedisMasterSlaveConnection}
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.{ReadFrom, RedisClient, RedisURI}
import monix.eval.Task
import monix.execution.Scheduler

import scala.jdk.CollectionConverters._
/**
  * An object that provides an aggregation of all the different Redis Apis.
  * They can be equally accessed independently or from this object.
  */
object RedisMasterSlave {

  def masterSlaveUtf(uri: String, readFrom: ReadFrom, redisUris: RedisURI*): StatefulRedisMasterSlaveConnection[String, String] = {
    val client = RedisClient.create()
    val connection = MasterSlave.connect(client, new Utf8StringCodec(), redisUris.asJava)
  }

  def create[K, V](uri: String, readFrom: ReadFrom, codec: RedisCodec[K, V], redisURI: RedisURI*): StatefulRedisMasterSlaveConnection[K, V] = {
    val client = RedisClient.create()
    MasterSlave.connect(client, codec, redisURI.asJava)
  }

  def create[K, V](redisURI: RedisURI, readNode: ReadFrom, codec: RedisCodec[K, V]): Resource[Task, RedisCmd[String, String]] =
    RedisCmd.acquireResource {
      for {
        client <-Task.evalAsync(RedisClient.create())
        connection <- Task.from(MasterSlave.connectAsync(client, codec, redisURI))
      } yield {
        connection.setReadFrom(readNode)
        RedisCmd.single(connection)
      }
    }

  def masterSlave(uri: RedisURI): RedisClient = {
    RedisClient.create(uri)
  }

  def masterSlave(clientResources: ClientResources): RedisClient = {
    ClientResources.builder().eventExecutorGroup()
    RedisClient.create(clientResources)
  }

  def masterSlave(clientResources: ClientResources, uri: String): RedisClient = {
    RedisClient.create(clientResources, uri)
  }

  def masterSlave(clientResources: ClientResources, uri: RedisURI): RedisClient = {
    RedisClient.create(clientResources, uri)
  }


}
