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
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.{ByteArrayCodec, StringCodec}
import monix.eval.Task

case class SingleConnection(uri: RedisUri) extends RedisConnection {

  /**
    *
    * @return
    */
  def connectUtf: Resource[Task, RedisCmd[String, String]] = {
    RedisCmd
      .connectResource[String, String, StatefulRedisConnection[String, String]] {
        for {
          client <- Task.evalAsync(RedisClient.create(uri.toJava))
          conn <- Task.from(client.connectAsync(StringCodec.UTF8, uri.toJava).toCompletableFuture)
        } yield conn
      }
      .evalMap(RedisCmd.single)
  }

  /**
    *
    * @param keyCodec
    * @param valueCodec
    * @tparam K
    * @tparam V
    *  @return
    */
  def connectUtf[K, V](
                        implicit keyCodec: Codec[K, String],
                        valueCodec: Codec[V, String]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .connectResource[K, V, StatefulRedisConnection[K, V]] {
        for {
          client <- Task.evalAsync(RedisClient.create(uri.toJava))
          conn <- Task.from(client.connectAsync(Codec(keyCodec, valueCodec, StringCodec.UTF8), uri.toJava).toCompletableFuture)
        } yield conn
      }
      .evalMap(RedisCmd.single)
  }

  /**
    *
    * @return
    */
  def connectByteArray: Resource[Task, RedisCmd[Array[Byte], Array[Byte]]] = {
    RedisCmd
      .connectResource[Array[Byte], Array[Byte], StatefulRedisConnection[Array[Byte], Array[Byte]]] {
        for {
          client <- Task.evalAsync(RedisClient.create(uri.toJava))
          conn <- Task.from {
            client.connectAsync(new ByteArrayCodec(), uri.toJava)
              .toCompletableFuture
          }
        } yield conn
      }
      .evalMap(RedisCmd.single)
  }

  /**
    *
    * @param keyCodec
    * @param valueCodec
    * @tparam K
    * @tparam V
    * @return
    */
  def connectByteArray[K, V](implicit keyCodec: Codec[K, Array[Byte]],
                              valueCodec: Codec[V, Array[Byte]]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .connectResource[K, V, StatefulRedisConnection[K, V]] {
        for {
          client <- Task.evalAsync(RedisClient.create(uri.toJava))
          conn <- Task.from {
            client.connectAsync(Codec(keyCodec, valueCodec, new ByteArrayCodec()), uri.toJava)
              .toCompletableFuture
          }
        } yield conn
      }
      .evalMap(RedisCmd.single)
  }
}
