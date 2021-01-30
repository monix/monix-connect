package monix.connect.redis.client

import cats.effect.Resource
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import monix.connect.redis.{AllCommands, KeyCommands}
import monix.eval.Task

case class RedisCmd[K, V](all: AllCommands[K, V, _], key: KeyCommands[K, V])

object RedisCmd {

  def single[K, V](connection: StatefulRedisConnection[K, V]): RedisCmd[K, V] = {
    val allCmd = new AllCommands(connection, connection.reactive)
    val keyCmd = new KeyCommands[K, V](connection.reactive)
    RedisCmd(allCmd, keyCmd)
  }

  def cluster[K, V](connection: StatefulRedisClusterConnection[K, V]): RedisCmd[K, V] = {
    val allCmd = new AllCommands(connection, connection.reactive)
    val keyCmd = new KeyCommands[K, V](connection.reactive)
    RedisCmd(allCmd, keyCmd)
  }

  private[redis] def acquireResource[K, V](createCmd: => Task[RedisCmd[K, V]]): Resource[Task, RedisCmd[K, V]] = {
    Resource.make(createCmd)(cmd => Task.evalAsync(cmd.all.close))
  }
}
