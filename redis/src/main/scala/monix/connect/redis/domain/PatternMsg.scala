package monix.connect.redis.domain

import io.lettuce.core.pubsub.api.reactive.PatternMessage

case class PatternMsg[K, V](pattern: K, channel: K, message: V)

object PatternMsg {

  def from[K, V](redisMsg: PatternMessage[K, V]): PatternMsg[K, V] = {
    PatternMsg(redisMsg.getPattern, redisMsg.getChannel, redisMsg.getMessage)
  }
}
