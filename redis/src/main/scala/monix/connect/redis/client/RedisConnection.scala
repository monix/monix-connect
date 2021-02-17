package monix.connect.redis.client

import cats.effect.Resource
import monix.eval.Task

trait RedisConnection {

  /**
    *
    */
  def utf: Resource[Task, RedisCmd[String, String]]

  /**
    *
    * @param keyCodec
    * @param valueCodec
    * @tparam K
    * @tparam V
    * @return
    */
  def utf[K, V](implicit keyCodec: Codec[K, String], valueCodec: Codec[V, String]): Resource[Task, RedisCmd[K, V]]

  /**
    *
    * @param keyCodec
    * @param valueCodec
    * @tparam K
    * @tparam V
    * @return
    */
  def byteArray[K, V](
    implicit keyCodec: Codec[K, Array[Byte]],
    valueCodec: Codec[V, Array[Byte]]): Resource[Task, RedisCmd[K, V]]

}
