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

import io.lettuce.core.RedisURI

import java.time.Duration
import scala.concurrent.duration.FiniteDuration

private[redis] case class RedisUri (
  host: String,
  port: Int,
  database: Option[Int] = None,
  password: Option[String] = None,
  ssl: Option[Boolean] = None,
  verifyPeer: Option[Boolean] = None,
  startTls: Option[Boolean] = None,
  timeout: Option[FiniteDuration] = None,
  sentinels: List[String] = List.empty,
  socket: Option[String] = None,
  sentinelMasterId: Option[String] = None,
  clientName: Option[String] = None) {

  def withDatabase(database: Int): RedisUri = copy(database = Some(database))
  def withPassword(password: String): RedisUri = copy(password = Some(password))
  def ssl(ssl: Boolean): RedisUri = copy(ssl = Some(ssl))
  def withVerifyPeer(verifyPeer: Boolean): RedisUri = copy(verifyPeer = Some(verifyPeer))
  def withStartTls(startTls: Boolean): RedisUri = copy(startTls = Some(startTls))
  def withTimeout(timeout: FiniteDuration): RedisUri = copy(timeout = Some(timeout))
  def withSentinels(sentinels: List[String]): RedisUri = copy(sentinels = sentinels)
  def withSocket(socket: String): RedisUri = copy(socket = Some(socket))
  def withSentinelMasterId(sentinelMasterId: String): RedisUri = copy(sentinelMasterId = Some(sentinelMasterId))
  def withClientName(clientName: String): RedisUri = copy(clientName = Some(clientName))

  private[redis] def toJava: RedisURI = {
    val builder = RedisURI.builder().withHost(host).withPort(port)
    database.map(builder.withDatabase)
    password.map(builder.withPassword)
    ssl.map(builder.withSsl)
    verifyPeer.map(builder.withVerifyPeer)
    startTls.map(builder.withStartTls)
    timeout.map(timeout => builder.withTimeout(Duration.ofMillis(timeout.toMillis)))
    sentinels.map(builder.withSentinel) //todo add sentinel host and port
    //socket.map(builder.socket) // todo figure out why it does not compile
    sentinelMasterId.map(builder.withSentinelMasterId)
    clientName.map(builder.withClientName)
    builder.build()
  }
}

object RedisUri {

  def apply(host: String, port: Int): RedisUri = RedisUri(host, port)

}
