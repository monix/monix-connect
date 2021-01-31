package monix.connect.redis.client

import io.lettuce.core.codec.RedisCodec

import java.nio.ByteBuffer

sealed trait Codec[T] {

  def encode(data: T): ByteBuffer
  def decode(bytes: ByteBuffer): T

}
trait KeyCodec[T] extends Codec[T]
trait ValueCodec[T] extends Codec[T]

object Codec {
  def forValue[T](encoder: T => ByteBuffer)(decoder: ByteBuffer => T): ValueCodec[T] = {
    new ValueCodec[T] {
      override def encode(data: T): ByteBuffer = encoder(data)
      override def decode(bytes: ByteBuffer): T = decoder(bytes)
    }
  }

  def forKey[T](encoder: T => ByteBuffer)(decoder: ByteBuffer => T): KeyCodec[T] = {
    new KeyCodec[T] {
      override def encode(data: T): ByteBuffer = encoder(data)
      override def decode(bytes: ByteBuffer): T = decoder(bytes)
    }
  }

  def apply[K, V](keyCodec: KeyCodec[K], valueCodec: ValueCodec[V]) = {
    new RedisCodec[K, V] {
      override def decodeKey(bytes: ByteBuffer): K = keyCodec.decode(bytes)

      override def encodeKey(key: K): ByteBuffer = keyCodec.encode(key

      override def decodeValue(bytes: ByteBuffer): V = valueCodec.decode(bytes)

      override def encodeValue(value: V): ByteBuffer = valueCodec.encode(value)
    }
  }

}