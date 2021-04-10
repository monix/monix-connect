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

/**
  * Contains connection details for the communication with standalone redis servers.
  * Allows to provide the database, client name, password and timeouts and more.
  *
  * You have different ways to create a [[RedisUri]]:
  *
  * ==Example==
  *
  * {{{
  *   //using an URI
  *   RedisUri("redis://localhost:6379")
  *
  *   //using host and port
  *   RedisUri("localhost", 6379)
  *
  *   //then you can pass custom options
  *   RedisUri("localhost", 6379)
  *   .withDatabase(1)
  *   .withPassword("Alice123") //this will normally come from a stored secret
  *   .withClientName("companyX")
  * }}}
  *
  */
class RedisUri(
  val uri: Either[String, (String, Int)],
  val database: Option[Int] = None,
  val password: Option[String] = None,
  val ssl: Option[Boolean] = None,
  val verifyPeer: Option[Boolean] = None,
  val startTls: Option[Boolean] = None,
  val timeout: Option[FiniteDuration] = None,
  val sentinels: List[String] = List.empty,
  val socket: Option[String] = None,
  val sentinelMasterId: Option[String] = None,
  val clientName: Option[String] = None) {

  def withDatabase(database: Int): RedisUri = copy(database = Some(database))
  def withPassword(password: String): RedisUri = copy(password = Some(password))
  def withSsl(ssl: Boolean): RedisUri = copy(ssl = Some(ssl))
  def withVerifyPeer(verifyPeer: Boolean): RedisUri = copy(verifyPeer = Some(verifyPeer))
  def withStartTls(startTls: Boolean): RedisUri = copy(startTls = Some(startTls))
  def withTimeout(timeout: FiniteDuration): RedisUri = copy(timeout = Some(timeout))
  def withSentinels(sentinels: List[String]): RedisUri = copy(sentinels = sentinels)
  def withSocket(socket: String): RedisUri = copy(socket = Some(socket))
  def withSentinelMasterId(sentinelMasterId: String): RedisUri = copy(sentinelMasterId = Some(sentinelMasterId))
  def withClientName(clientName: String): RedisUri = copy(clientName = Some(clientName))

  private[redis] def toJava: RedisURI = {
    val redisUri = uri.map { case (host, port) => RedisURI.create(host, port) }.left
      .map(uri => RedisURI.create(uri))
      .merge
    database.map(_ => redisUri.setDatabase(_))
    password.map(pass => redisUri.setPassword(pass))
    ssl.map(_ => redisUri.setSsl(_))
    verifyPeer.map(redisUri.setVerifyPeer(_))
    startTls.map(_ => redisUri.setStartTls(_))
    timeout.map(timeout => redisUri.setTimeout(Duration.ofMillis(timeout.toMillis)))
    sentinels.map(_ => redisUri.setSentinelMasterId(_)) //todo add sentinel host and port
    socket.map(_ => redisUri.setSocket(_))
    sentinelMasterId.map(_ => redisUri.setSentinelMasterId(_))
    clientName.map(_ => redisUri.setClientName(_))
    redisUri
  }

  private def copy(
    uri: Either[String, (String, Int)] = this.uri,
    database: Option[Int] = this.database,
    password: Option[String] = this.password,
    ssl: Option[Boolean] = this.ssl,
    verifyPeer: Option[Boolean] = this.verifyPeer,
    startTls: Option[Boolean] = this.startTls,
    timeout: Option[FiniteDuration] = this.timeout,
    sentinels: List[String] = this.sentinels,
    socket: Option[String] = this.socket,
    sentinelMasterId: Option[String] = this.sentinelMasterId,
    clientName: Option[String] = this.clientName): RedisUri = {
    this
  }
}

object RedisUri {

  /**
    * Creates a [[RedisUri]] from host and port.
    *
    * ==Example==
    *
    * {{{
    *   RedisUri("localhost", 6379)
    *   .withDatabase(1)
    *   .withPassword("Alice123") //this will normally come from a stored secret
    *   .withClientName("companyX")
    * }}}
    */
  def apply(host: String, port: Int): RedisUri = new RedisUri(Right(host, port))

  /**
    * Creates a [[RedisUri]] from a the plain string uri.
    *
    * ==Example==
    *
    * {{{
    *   RedisUri("localhost", 6379)
    *   .withDatabase(1)
    *   .withPassword("Alice123") //this will normally come from a stored secret
    *   .withClientName("companyX")
    * }}}
    *
    * ==Uri Syntax==
    *
    * - Redis Standalone
    * redis://[password@]host [: port][/database][? [timeout=timeout[d|h|m|s|ms|us|ns]] [ &database=database] [&clientName=clientName]]
    *
    * - Redis Standalone (SSL)
    * rediss://[password@]host [: port][/database][? [timeout=timeout[d|h|m|s|ms|us|ns]] [ &database=database] [&clientName=clientName]]
    *
    * @see <a href="https://github.com/lettuce-io/lettuce-core/wiki/Redis-URI-and-connection-details">RedisUri connection details.</a>,
    */
  def apply(uri: String): RedisUri = new RedisUri(Left(uri))

}
