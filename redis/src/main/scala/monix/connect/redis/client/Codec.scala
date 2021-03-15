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

import io.lettuce.core.codec.RedisCodec

import java.nio.ByteBuffer

/**
  * A [[Codec]] encodes keys and values sent to Redis,
  * and decodes keys and values in the command output.
  */
sealed trait Codec[T, R] {
  def encode(dec: T): R
  def decode(enc: R): T
}

trait UtfCodec[T] extends Codec[T, String]
trait BytesCodec[T] extends Codec[T, Array[Byte]]

object Codec {

  /**
    * Creates an instance of [[Codec]] that will be used in our
    * [[RedisConnection connectUtf]] to encode keys, values or
    * both, sent to Redis, and decodes them from the command output.
    *
    * @param encoder function to encode from [[T]] to [[String]].
    * @param decoder function to decode from [[String]] to [[T]]
    * @tparam T type that we want to encode as UTF in Redis.
    *
    */
  def utf[T](encoder: T => String, decoder: String => T): UtfCodec[T] = {
    new UtfCodec[T] {
      override def encode(dec: T): String = encoder(dec)
      override def decode(enc: String): T = decoder(enc)
    }
  }

  /**
    * Creates an instance of [[Codec]] that will be used in our
    * [[RedisConnection connectByteArray]] to encode keys, values or
    * both, sent to Redis, and decodes them from the command output.
    *
    * @param encoder function to encode from [[T]] to [[Array[Byte] ]].
    * @param decoder function to decode from [[Array[Byte] ]] to [[T]]
    * @tparam T type that we want to encode as ByteArray in Redis.
    *
    */
  def byteArray[T](encoder: T => Array[Byte], decoder: Array[Byte] => T): BytesCodec[T] = {
    new BytesCodec[T] {
      override def encode(dec: T): Array[Byte] = encoder(dec)
      override def decode(enc: Array[Byte]): T = decoder(enc)
    }
  }

  private[redis] def apply[K, V, R](
    keyCodec: Codec[K, R],
    valueCodec: Codec[V, R],
    redisCodec: RedisCodec[R, R]): RedisCodec[K, V] = {
    new RedisCodec[K, V] {
      override def decodeKey(bytes: ByteBuffer): K = keyCodec.decode(redisCodec.decodeKey(bytes))
      override def encodeKey(key: K): ByteBuffer = redisCodec.encodeKey(keyCodec.encode(key))
      override def decodeValue(bytes: ByteBuffer): V = valueCodec.decode(redisCodec.decodeKey(bytes))
      override def encodeValue(value: V): ByteBuffer = redisCodec.encodeKey(valueCodec.encode(value))
    }
  }

}
