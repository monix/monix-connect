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

package monix.connect.redis

import io.lettuce.core.KeyValue
import io.lettuce.core.api.StatefulRedisConnection
import monix.connect.redis.client.RedisUri
import monix.eval.Task
import monix.reactive.Observable
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

class RedisUriSpec
  extends AnyFlatSpec with Matchers with IdiomaticMockito with BeforeAndAfterEach with BeforeAndAfterAll {

  s"$RedisUri" should "allow to update its properties" in {
    val password = "password"
    val database = 123
    val ssl: Boolean = true
    val verifyPeer: Boolean = true
    val startTls: Boolean = true
    val timeout: FiniteDuration = 1.second
    val sentinels: List[String] = List("sentinel")
    val socket: String = "someSocket"
    val sentinelMasterId: String = "1234"
    val clientName: String = "myClient"
    val redisUri = RedisUri("localhost", 123)
    val updatedRedisUri = redisUri
      .withDatabase(database)
      .withPassword(password)
      .withSsl(ssl)
      .withVerifyPeer(verifyPeer)
      .withStartTls(startTls)
      .withTimeout(timeout)
      .withSentinels(sentinels)
      .withSocket(socket)
      .withSentinelMasterId(sentinelMasterId)
      .withClientName(clientName)

    updatedRedisUri.database shouldBe Some(database)
    updatedRedisUri.password shouldBe Some(password)
    updatedRedisUri.ssl shouldBe Some(ssl)
    updatedRedisUri.verifyPeer shouldBe Some(verifyPeer)
    updatedRedisUri.startTls shouldBe Some(startTls)
    updatedRedisUri.timeout shouldBe Some(timeout)
    updatedRedisUri.sentinels shouldBe sentinels
    updatedRedisUri.socket shouldBe Some(socket)
    updatedRedisUri.sentinelMasterId shouldBe Some(sentinelMasterId)
    updatedRedisUri.clientName shouldBe Some(clientName)
  }

}
