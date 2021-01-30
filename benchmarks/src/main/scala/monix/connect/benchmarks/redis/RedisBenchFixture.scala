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

package monix.connect.benchmarks.redis

import cats.effect.{Blocker, ContextShift, IO, Timer}
import dev.profunktor._
import dev.profunktor.redis4cats.effect.Log.NoOp._
import fs2.io.tcp.SocketGroup
import io.chrisdavenport.rediculous.{Redis, RedisConnection}
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import laserdisc._
import monix.connect.redis
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.ExecutionContext

trait RedisBenchFixture {
  type RedisIO[A] = Redis[IO, A]

  final val RedisHost = "localhost"
  final val RedisPort = 6379
  final val redisUrl = s"redis://$RedisHost:$RedisPort"

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  implicit val connection: StatefulRedisConnection[String, String] = RedisClient.create(redisUrl).connect()

  val redis4catsConn = redis4cats.Redis[IO].utf8(redisUrl)

  val redicolousConn =
    for {
      blocker <- Blocker[IO]
      sg      <- SocketGroup[IO](blocker)
      c       <- RedisConnection.queued[IO](sg, RedisHost, RedisPort, maxQueued = 10000, workers = 2)
    } yield c

  val laserdConn = fs2.RedisClient.to(Host.unsafeFrom(RedisHost), Port.unsafeFrom(RedisPort))

  val maxKey: Int = 5000

  def flushdb = redis.$Commands.flushdbAsync().runSyncUnsafe()
}
