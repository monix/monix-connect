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
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.codec.{ByteArrayCodec, StringCodec, Utf8StringCodec}
import monix.eval.Task
import io.lettuce.core.cluster.api.{StatefulRedisClusterConnection => RedisClusterConnection}

import scala.jdk.CollectionConverters._

case class ClusterConnection(uris: List[RedisUri]) extends RedisConnection {

  def connectUtf: Resource[Task, RedisCmd[String, String]] = {
    RedisCmd
      .connectResource[String, String, RedisClusterConnection[String, String]] {
        for {
          client <- Task.now(RedisClusterClient.create(uris.map(_.toJava).asJava))
          _ <- Task.now(client.getPartitions)
          conn <- Task.from(client.connectAsync(StringCodec.UTF8).toCompletableFuture)
        } yield conn
      }.evalMap(RedisCmd.cluster)
  }


  def connectUtf[K, V](implicit keyCodec: Codec[K, String], valueCodec: Codec[V, String]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .connectResource[K, V, RedisClusterConnection[K, V]] {
        for {
          client <- Task.now(RedisClusterClient.create(uris.map(_.toJava).asJava))
          _ <- Task.now(client.getPartitions)
          conn <- Task.from {
            client.connectAsync(Codec(keyCodec, valueCodec, StringCodec.UTF8)).toCompletableFuture
          }
        } yield conn
      }
      .evalMap(RedisCmd.cluster)
  }

  def connectByteArray: Resource[Task, RedisCmd[Array[Byte], Array[Byte]]] = {
    RedisCmd
      .connectResource[Array[Byte], Array[Byte], RedisClusterConnection[Array[Byte], Array[Byte]]] {
        for {
          client <- Task.now(RedisClusterClient.create(uris.map(_.toJava).asJava))
          _ <- Task.now(client.getPartitions)
          conn <- Task.from {
            client.connectAsync(new ByteArrayCodec()).toCompletableFuture
          }
        } yield conn
      }
      .evalMap(RedisCmd.cluster)
  }

  def connectByteArray[K, V](
                              implicit keyCodec: Codec[K, Array[Byte]],
                              valueCodec: Codec[V, Array[Byte]]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .connectResource[K, V, RedisClusterConnection[K, V]] {
        for {
          client <- Task.now(RedisClusterClient.create(uris.map(_.toJava).asJava))
          _ <- Task.eval(client.getPartitions)
          conn <- Task.from {
            client.connectAsync(Codec(keyCodec, valueCodec, new ByteArrayCodec())).toCompletableFuture
          }
        } yield conn
      }
      .evalMap(RedisCmd.cluster)
  }

}
