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
import io.lettuce.core.codec.{RedisCodec, Utf8StringCodec}
import io.lettuce.core.masterslave.{MasterSlave, StatefulRedisMasterSlaveConnection}
import io.lettuce.core.{ReadFrom, RedisClient, RedisURI}
import monix.eval.Task

import scala.jdk.CollectionConverters._
/**
  * An object that provides an aggregation of all the different Redis Apis.
  * They can be equally accessed independently or from this object.
  */
object RedisMasterSlave {

  /*
  def masterSlaveUtf(uri: String, readFrom: ReadFrom, redisUris: RedisURI*): StatefulRedisMasterSlaveConnection[String, String] = {
    ???
  }

  def create[K, V](uri: String, readFrom: ReadFrom, codec: RedisCodec[K, V], redisURI: RedisURI*): StatefulRedisMasterSlaveConnection[K, V] = {
    val client = RedisClient.create()
    MasterSlave.connect(client, codec, redisURI.asJava)
  }

  def create[K, V](redisURIs: List[RedisURI], readNode: ReadFrom, codec: RedisCodec[K, V]): Resource[Task, RedisCmd[K, V]] =
    RedisCmd.connectResource {
      for {
        client <-Task.evalAsync(RedisClient.create())
        connection <- Task.from(MasterSlave.connectAsync(client, codec, redisURIs.asJava))
        cmd <- {
          connection.setReadFrom(readNode)
          RedisCmd.single(connection)
        }
      } yield cmd
    }

 */
}
