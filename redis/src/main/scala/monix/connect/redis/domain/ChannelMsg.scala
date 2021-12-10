package monix.connect.redis.domain

import io.lettuce.core.pubsub.api.reactive.ChannelMessage

case class ChannelMsg[K, V](channel: K, message: V)

object ChannelMsg {

  def from[K, V](redisMsg: ChannelMessage[K, V]): ChannelMsg[K, V] = {
    ChannelMsg(redisMsg.getChannel, redisMsg.getMessage)
  }

}
