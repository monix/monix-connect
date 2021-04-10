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
import io.lettuce.core.codec.{ByteArrayCodec, StringCodec}
import monix.eval.Task
import io.lettuce.core.cluster.api.{StatefulRedisClusterConnection => RedisClusterConnection}

import scala.jdk.CollectionConverters._

/**
  * Represents a connection to a set of redis servers (cluster),
  * extending the [[RedisConnection]] interface that
  * defines the set of methods to create a connection that can
  * encode in `UTF` and `ByteArray` with custom [[Codec]]s.
  */
class ClusterConnection(uris: List[RedisUri]) extends RedisConnection {

  def connectUtf: Resource[Task, RedisCmd[String, String]] = {
    RedisCmd
      .createResource[String, String, RedisClusterConnection[String, String]] {
        for {
          client <- Task.evalAsync(RedisClusterClient.create(uris.map(_.toJava).asJava))
          _      <- Task.evalAsync(client.getPartitions())
          conn   <- Task.from(client.connectAsync(StringCodec.UTF8).toCompletableFuture)
        } yield (client, conn)
      }
      .evalMap(RedisCmd.cluster)
  }

  def connectUtf[K, V](implicit keyCodec: UtfCodec[K], valueCodec: UtfCodec[V]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .createResource[K, V, RedisClusterConnection[K, V]] {
        for {
          client <- Task.evalAsync(RedisClusterClient.create(uris.map(_.toJava).asJava))
          _      <- Task.evalAsync(client.getPartitions)
          conn <- Task.from {
            client.connectAsync(Codec(keyCodec, valueCodec, StringCodec.UTF8)).toCompletableFuture
          }
        } yield (client, conn)
      }
      .evalMap(RedisCmd.cluster)
  }

  def connectByteArray: Resource[Task, RedisCmd[Array[Byte], Array[Byte]]] = {
    RedisCmd
      .createResource[Array[Byte], Array[Byte], RedisClusterConnection[Array[Byte], Array[Byte]]] {
        for {
          client <- Task.evalAsync(RedisClusterClient.create(uris.map(_.toJava).asJava))
          _      <- Task.evalAsync(client.getPartitions)
          conn <- Task.from {
            client.connectAsync(new ByteArrayCodec()).toCompletableFuture
          }
        } yield (client, conn)
      }
      .evalMap(RedisCmd.cluster)
  }

  def connectByteArray[K, V](
    implicit keyCodec: BytesCodec[K],
    valueCodec: BytesCodec[V]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .createResource[K, V, RedisClusterConnection[K, V]] {
        for {
          client <- Task.evalAsync(RedisClusterClient.create(uris.map(_.toJava).asJava))
          _      <- Task.evalAsync(client.getPartitions)
          conn <- Task.from {
            client.connectAsync(Codec(keyCodec, valueCodec, new ByteArrayCodec())).toCompletableFuture
          }
        } yield (client, conn)
      }
      .evalMap(RedisCmd.cluster)
  }

}
