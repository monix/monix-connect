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
import io.lettuce.core.api.{StatefulRedisConnection => RedisConnection}
import io.lettuce.core.RedisClient
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.codec.{ByteArrayCodec, Utf8StringCodec}
import monix.eval.Task
import monix.reactive.Observable

import scala.jdk.CollectionConverters._

/**
  * An object that provides an aggregation of all the different Redis Apis.
  * They can be equally accessed independently or from this object.
  */
object Redis {

  def apply(uri: String): Resource[Task, RedisCmd[String, String]] =
    RedisCmd
      .connectResource[String, String, RedisConnection[String, String]] {
        Task.evalAsync(RedisClient.create(uri).connect)
      }
      .evalMap(RedisCmd.single)

  def apply(uri: RedisUri): Resource[Task, RedisCmd[String, String]] =
    RedisCmd
      .connectResource[String, String, RedisConnection[String, String]] {
        Task.evalAsync(RedisClient.create(uri.toJava).connect)
      }
      .evalMap(RedisCmd.single)

  def utfCodec[K, V](
    uri: String)(implicit keyCodec: Codec[K, String], valueCodec: Codec[V, String]): Resource[Task, RedisCmd[K, V]] =
    RedisCmd
      .connectResource[K, V, RedisConnection[K, V]] {
        Task.evalAsync(RedisClient.create(uri).connect(Codec(keyCodec, valueCodec, new Utf8StringCodec())))
      }
      .evalMap(RedisCmd.single)

  def byteArrayCodec[K, V](uri: String)(
    implicit keyCodec: Codec[K, Array[Byte]],
    valueCodec: Codec[V, Array[Byte]]): Resource[Task, RedisCmd[K, V]] =
    RedisCmd
      .connectResource[K, V, RedisConnection[K, V]] {
        Task.evalAsync(RedisClient.create(uri).connect(Codec(keyCodec, valueCodec, new ByteArrayCodec())))
      }
      .evalMap(RedisCmd.single)
  //def connectWithCodec[K, V](uri: RedisURI)(implicit keyCodec: Codec[K, Array[Byte]], valueCodec: Codec[V]): Resource[Task, RedisCmd[K, V]] =
  //   RedisCmd.connectResource[K, V, RedisConnection[K, V]] {
  //     Task.evalAsync(RedisClient.create(uri).connect(Codec(keyCodec, valueCodec)))
  //   }.evalMap(RedisCmd.single)

  def cluster[K, V](uris: List[RedisUri]): ClusterConnection =
    ClusterConnection(uris)


}
