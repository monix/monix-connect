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
import monix.eval.Task

/**
  * Trait that defines the generic set of methods to connect with Redis.
  * It supports encoding and decoding in Utf and ByteArray with custom codecs.
  *
  * It currently supports [[ClusterConnection]] and [[SingleConnection]] for cluster
  * and single connections to Redis.
  */
trait RedisConnection {

  /**
    * Connect asynchronously to a Redis Cluster.
    * Encodes and decodes [[String]] keys and values in `UTF` Charset.
    *
    * @return A `Resource` that acquires a redis connection and exposes the usage
    *         of the [[RedisCmd]] [[String]] commands that will be released afterwards.
    */
  def connectUtf: Resource[Task, RedisCmd[String, String]]

  /**
    * Connect asynchronously to a Redis Cluster.
    * It requires a codec for keys and values that will encode/decodes
    * respectively as [[K]] and [[V]] to/from `UTF` Charset.
    *
    * @see [[ClusterConnection connectUtf()]] and [[SingleConnection connectUtf]] for
    *      respective examples.
    *
    * @param keyCodec a [[UtfCodec]] to encode/decode the key to `UTF`.
    * @param valueCodec a [[UtfCodec]] to encode/decode the value to `UTF`.
    * @tparam K the connection's key type.
    * @tparam V the connection's value type.
    * @return A `Resource` that acquires a redis connection and exposes the usage
    *         of the [[RedisCmd[K, V] ]].
    */
  def connectUtf[K, V](
    implicit keyCodec: Codec[K, String],
    valueCodec: Codec[V, String]): Resource[Task, RedisCmd[K, V]]

  /**
    * Connect asynchronously to a Redis Cluster.
    * Encodes and decodes [[Array[Byte] ]] keys and values.
    *
    * @return A `Resource` that acquires a redis connection and exposes the usage
    *         of the [[RedisCmd]] [[Array[Byte] ]] commands that will be released afterwards.
    */
  def connectByteArray: Resource[Task, RedisCmd[Array[Byte], Array[Byte]]]

  /**
    * Connect asynchronously to a Redis.
    * It requires a codec for keys and values that will encode/decode
    * respectively as [[K]] and [[V]] to/from [[Array[Byte] ]] Charset.
    *
    * @see [[ClusterConnection connectByteArray()]] and [[SingleConnection connectByteArray]] for
    *      respective examples.
    *
    * @param keyCodec a [[BytesCodec]] to encode/decode the key to [[Array[Byte] ]] .
    * @param valueCodec a [[BytesCodec]] to encode/decode the value to [[Array[Byte] ]] .
    * @tparam K the connection's key type.
    * @tparam V the connection's value type.
    * @return A `Resource` that acquires a redis connection and exposes the usage
    *         of the [[RedisCmd[K, V] ]].
    */
  def connectByteArray[K, V](
    implicit keyCodec: Codec[K, Array[Byte]],
    valueCodec: Codec[V, Array[Byte]]): Resource[Task, RedisCmd[K, V]]

}