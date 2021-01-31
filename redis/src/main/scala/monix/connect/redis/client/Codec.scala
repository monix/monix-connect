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

import io.lettuce.core.codec.{ByteArrayCodec, RedisCodec, Utf8StringCodec}

import java.nio.ByteBuffer

sealed trait Codec[T, R] {
  def encode(dec: T): R
  def decode(enc: R): T
}

trait UtfCodec[T] extends Codec[T, String]
trait BytesCodec[T] extends Codec[T, Array[Byte]]

object Codec {
  def utf[T](encoder: T => String, decoder: String => T): UtfCodec[T] = {
    new UtfCodec[T] {
      override def encode(dec: T): String = encoder(dec)
      override def decode(enc: String): T = decoder(enc)
    }
  }

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

  private[redis] def apply[K, V](keyCodec: Codec[K, String], valueCodec: Codec[V, String]): RedisCodec[K, V] =
    apply(keyCodec, valueCodec, new Utf8StringCodec())

  private[redis] def byteArray[K, V](keyCodec: Codec[K, Array[Byte]], valueCodec: Codec[V, Array[Byte]]): RedisCodec[K, V] =
    apply(keyCodec, valueCodec, new ByteArrayCodec())
}
