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
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.RedisURI
import io.lettuce.core.cluster.api.{StatefulRedisClusterConnection => ClusterConnection}
import monix.eval.Task

import scala.jdk.CollectionConverters._

/**
  * An object that provides an aggregation of all the different Redis Apis.
  * They can be equally accessed independently or from this object.
  */
object RedisCluster {
  /*
  def apply(uri: String): Resource[Task, RedisCmd[String, String]] =
    RedisCmd
      .connectResource[String, String, ClusterConnection[String, String]] {
        Task.evalAsync(RedisClusterClient.create(uri).connect)
      }
      .evalMap(RedisCmd.cluster)

  def apply[K, V](uri: RedisURI): Resource[Task, RedisCmd[String, String]] =
    RedisCmd
      .connectResource[K, V, ClusterConnection[String, String]] {
        Task.evalAsync(RedisClusterClient.create(uri).connect)
      }
      .evalMap(RedisCmd.cluster)

  def withCodec(uris: List[RedisUri]): Resource[Task, RedisCmd[String, String]] =
    RedisCmd
      .connectResource[String, String, ClusterConnection[String, String]] {
        Task.evalAsync(RedisClusterClient.create(uris.map(_.toJava).asJava).connect)
      }
      .evalMap(RedisCmd.cluster)

  def create[K, V](uri: RedisURI)(implicit keyCodec: Codec[K, V], valueCodec: Codec[K, V]): Resource[Task, RedisCmd[K, V]] =
    RedisCmd
      .connectResource[K, V, ClusterConnection[K, V]] {
        Task.evalAsync(RedisClusterClient.create(uri).connect(codec))
      }
      .evalMap(RedisCmd.cluster)
 */
}
