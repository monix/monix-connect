package monix.connect.redis.client

import cats.effect.Resource
import io.lettuce.core.RedisClient
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.api.{StatefulRedisConnection => RedisConnection}
import io.lettuce.core.codec.{ByteArrayCodec, Utf8StringCodec}
import monix.eval.Task

case class SingleConnection(uri: RedisUri) {

  def utf: Resource[Task, RedisCmd[String, String]] = {
    RedisCmd
      .connectResource[String, String, RedisConnection[String, String]] {
        Task.evalAsync(RedisClient.create(uri.toJava).connect)
      }
      .evalMap(RedisCmd.single)
  }

  def utf[K, V](
    implicit keyCodec: Codec[K, String],
    valueCodec: Codec[V, String]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .connectResource[K, V, RedisConnection[K, V]] {
        Task.evalAsync(RedisClient.create(uri.toJava).connect(Codec(keyCodec, valueCodec, new Utf8StringCodec())))
      }
      .evalMap(RedisCmd.single)
  }

  def byteArray[K, V](
    implicit keyCodec: Codec[K, Array[Byte]],
    valueCodec: Codec[V, Array[Byte]]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .connectResource[K, V, RedisConnection[K, V]] {
        Task.evalAsync(RedisClient.create(uri.toJava).connect(Codec(keyCodec, valueCodec, new ByteArrayCodec())))
      }
      .evalMap(RedisCmd.single)
  }
}
