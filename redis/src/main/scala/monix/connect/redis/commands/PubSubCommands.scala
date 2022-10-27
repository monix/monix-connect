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

import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands
import monix.connect.redis.domain.{ChannelMsg, PatternMsg}
import monix.eval.Task
import monix.reactive.Observable

/**
  * Exposes the set of redis pub/sub commands available.
  * @see <a href="https://redis.io/commands#pubsub">Pub/Sub commands reference</a>.
  */
final class PubSubCommands[K, V] private[redis] (reactiveCmd: RedisPubSubReactiveCommands[K, V]) {

  /**
    * Subscribes to the specific [[channel]] to listen for new events.
    * The subscription is necessary to later on start consuming messages
    * published to the given channel with [[observeChannel]].
    *
    * @return A [[Task]] that completes as soon as the subscription is registered.
    */
  def subscribe(channel: K): Task[Unit] = Task.fromReactivePublisher(reactiveCmd.subscribe(channel)).void

  /**
    * Subscribes to the specific [[channels]] to listen for new events.
    * The subscription is necessary to later on start consuming messages
    * published to the given channels with [[observeChannels]].
    *
    * @return A [[Task]] that completes as soon as the subscription is registered.
    */
  def subscribe(channels: List[K]): Task[Unit] = {
    Task.fromReactivePublisher(reactiveCmd.subscribe(channels: _*)).void
  }

  /**
    * Subscribes to the specific [[pattern]] to listen for new events.
    * The subscription is  necessary to later on start consuming messages published
    * to the given pattern with [[observePattern]] or [[observeChannel]].
    *
    * @return A [[Task]] that completes as soon as the subscription is registered.
    */
  def pSubscribe(pattern: K): Task[Unit] = {
    Task.fromReactivePublisher(reactiveCmd.psubscribe(pattern)).void
  }

  /**
    * Subscribes to the specific [[patterns]] to listen for new events.
    * The subscription is necessary to later on start consuming messages published
    * to the given patterns with [[observePatterns]] or [[observeChannels]].
    *
    * @return A [[Task]] that completes as soon as the subscription is registered.
    */
  def pSubscribe(patterns: List[K]): Task[Unit] = {
    Task.fromReactivePublisher(reactiveCmd.psubscribe(patterns: _*)).void
  }

  /**
    * Starts listening for all channel messages which
    * the redis client is [[subscribe]]d to.
    *
    * @return An [[Observable]] that emits [[ChannelMsg]]s.
    */
  def observeChannels: Observable[ChannelMsg[K, V]] = {
    Observable.fromReactivePublisher(reactiveCmd.observeChannels()).map(ChannelMsg.from)
  }

  /**
    * Starts consuming channel messages only from the specified channel.
    *
    * @return An [[Observable]] that emits messages of type [[V]].
    */
  def observeChannel(channel: K): Observable[V] = {
    Observable
      .fromReactivePublisher(reactiveCmd.observeChannels())
      .filter(_.getChannel == channel)
      .map(_.getMessage)
  }

  /**
    * Starts consuming all pattern messages which
    * the redis client is [[subscribe]]d to.
    *
    * @return An [[Observable]] that emits [[PatternMsg]].
    */
  def observePatterns: Observable[PatternMsg[K, V]] = {
    Observable.fromReactivePublisher(reactiveCmd.observePatterns()).map(PatternMsg.from)
  }

  /**
    * Starts consuming pattern messages only from the specified pattern.
    *
    * @return An [[Observable]] that emits [[PatternMsg]].
    */
  def observePattern(pattern: K): Observable[PatternMsg[K, V]] = {
    Observable
      .fromReactivePublisher(reactiveCmd.observePatterns())
      .filter(_.getPattern == pattern)
      .map(PatternMsg.from)
  }

  /**
    * Publishes a new message to the specified channel.
    */
  def publish(channel: K, message: V): Task[Long] = {
    Task.fromReactivePublisher(reactiveCmd.publish(channel, message)).flatMap {
      case Some(v) => Task.pure(v)
      case None => Task.raiseError(new IllegalStateException(s"Unknown failure publishing to channel $channel."))
    }
  }

}

private[redis] object PubSubCommands {
  def apply[K, V](reactiveCmd: RedisPubSubReactiveCommands[K, V]): PubSubCommands[K, V] =
    new PubSubCommands[K, V](reactiveCmd)
}
