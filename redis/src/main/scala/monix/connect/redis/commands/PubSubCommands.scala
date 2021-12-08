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

package monix.connect.redis.commands

import io.lettuce.core.api.reactive.{RedisSetReactiveCommands, RedisStringReactiveCommands}
import io.lettuce.core.cluster.pubsub.api.reactive.RedisClusterPubSubReactiveCommands
import io.lettuce.core.pubsub.RedisPubSubListener
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands
import io.lettuce.core.pubsub.api.reactive.{ChannelMessage, PatternMessage, RedisPubSubReactiveCommands}
import monix.connect.redis.kvToTuple
import monix.eval.Task
import monix.reactive.Observable
import reactor.core.publisher.FluxSink.OverflowStrategy
import reactor.core.publisher.Mono

import java.lang
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * Exposes the set of redis string commands available.
  * @see <a href="https://redis.io/commands#string">String commands reference</a>.
  *
  * @note Does not support `bitfield`.
  */
final class PubSubCommands[K, V] private[redis] (reactiveCmd: RedisPubSubReactiveCommands[K, V]) {

  def subscribe(channel: K): Task[Unit] = {
    Task.fromReactivePublisher(reactiveCmd.subscribe(channel)).void
  }

  def subscribe(channels: List[K]): Task[Unit] = {
    Task.fromReactivePublisher(reactiveCmd.subscribe(channels: _*)).void
  }

  def pSubscribe(pattern: K): Task[Unit] = {
    Task.fromReactivePublisher(reactiveCmd.psubscribe(pattern)).void
  }

  def pSubscribe(pattern: List[K]): Task[Unit] = {
    Task.fromReactivePublisher(reactiveCmd.psubscribe(pattern: _*)).void
  }

  def observeChannels: Observable[ChannelMessage[K, V]] = {
    Observable.fromReactivePublisher(reactiveCmd.observeChannels())
  }

  def observeChannel(channel: K): Observable[V] = {
    Observable.fromReactivePublisher(reactiveCmd.observeChannels()).filter(_.getChannel == channel).map(_.getMessage)
  }

  def observePatterns: Observable[PatternMessage[K, V]] = {
    Observable.fromReactivePublisher(reactiveCmd.observePatterns())
  }

  def observePattern(pattern: K): Observable[PatternMessage[K, V]] = {
    Observable.fromReactivePublisher(reactiveCmd.observePatterns()).filter(_.getPattern == pattern)
  }

  def publish(channel: K, message: V): Task[Long] = {
    Task.fromReactivePublisher(reactiveCmd.publish(channel, message)).flatMap {
      case Some(v) => Task.pure(v)
      case None => Task.raiseError(new IllegalStateException(s"Failed publishing to channel $channel."))
    }
  }
}

private[redis] object PubSubCommands {
  def apply[K, V](reactiveCmd: RedisPubSubReactiveCommands[K, V]): PubSubCommands[K, V] =
    new PubSubCommands[K, V](reactiveCmd)
}
