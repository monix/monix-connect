/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import monix.connect.redis.Redis
import monix.execution.Scheduler.Implicits.global

trait RedisBenchFixture {
  final val redisUrl = "redis://localhost:6379"
  implicit val connection: StatefulRedisConnection[String, String] = RedisClient.create(redisUrl).connect()

  val maxKey: Int = 5000
  val maxField: Int = 500
  val maxValue: Int = 50000

  var key: Int = 0
  var field: Int = 0
  var value: Int = 0

  def flushdb = Redis.flushdbAsync().runSyncUnsafe()

  def getNextKey = {
    if (key >= maxKey)
      key = 0
    key += 1
    key
  }

  def getNextField = {
    if (field >= maxField)
      field = 0
    field += 1
    field
  }

  def getNextValue = {
    if (value >= maxValue)
      value = 0
    value += 1
    value
  }

}
