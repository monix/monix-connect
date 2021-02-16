package monix.connect.redis.client

import cats.effect.Resource
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.codec.{ByteArrayCodec, Utf8StringCodec}
import monix.eval.Task
import io.lettuce.core.cluster.api.{StatefulRedisClusterConnection}

import scala.jdk.CollectionConverters._

case class ClusterConnection(uris: List[RedisUri]) {

  def utf: Resource[Task, RedisCmd[String, String]] = {
    RedisCmd
      .connectResource[String, String, StatefulRedisClusterConnection[String, String]] {
        Task.evalAsync {
          RedisClusterClient
            .create(uris.map(_.toJava).asJava)
            .connect()
        }
      }
      .evalMap(RedisCmd.cluster)
  }

  def utf[K, V](implicit keyCodec: Codec[K, String], valueCodec: Codec[V, String]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .connectResource[K, V, StatefulRedisClusterConnection[K, V]] {
        Task.evalAsync {
          RedisClusterClient
            .create(uris.map(_.toJava).asJava)
            .connect(Codec(keyCodec, valueCodec, new Utf8StringCodec()))
        }
      }
      .evalMap(RedisCmd.cluster)
  }

  def byteArray[K, V](implicit keyCodec: Codec[K, Array[Byte]], valueCodec: Codec[V, Array[Byte]]): Resource[Task, RedisCmd[K, V]] = {
    RedisCmd
      .connectResource[K, V, StatefulRedisClusterConnection[K, V]] {
        Task.evalAsync {
          RedisClusterClient
            .create(uris.map(_.toJava).asJava)
            .connect(Codec(keyCodec, valueCodec, new ByteArrayCodec()))
        }
      }
      .evalMap(RedisCmd.cluster)
  }
}
