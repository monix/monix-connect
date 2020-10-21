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

import monix.connect.redis.RedisList
import monix.execution.Scheduler.Implicits.global
import org.openjdk.jmh.annotations._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisListsBenchmark extends RedisBenchFixture {

  @Setup
  def setup(): Unit = {
    flushdb
    (1 to maxKey).foreach { key =>
      val value = getNextValue.toString
      RedisList.rpush(key.toString, value).runSyncUnsafe()
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def listLengthReader(): Unit = {
    val key = getNextKey.toString
    RedisList.llen(key).runSyncUnsafe()
  }

  @Benchmark
  def listByIndexReader(): Unit = {
    val key = getNextKey.toString
    RedisList.lindex(key, 0).runSyncUnsafe()
  }

  @Benchmark
  def listRangeReader(): Unit = {
    val key = getNextKey.toString
    RedisList.lrange(key, 0, 1).toListL.runSyncUnsafe()
  }

}
