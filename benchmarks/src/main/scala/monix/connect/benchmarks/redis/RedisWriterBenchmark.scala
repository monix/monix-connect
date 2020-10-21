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

import java.util.Optional

import io.lettuce.core.ScoredValue
import monix.connect.redis.{Redis, RedisHash, RedisList, RedisSet, RedisSortedSet, RedisString}
import monix.execution.Scheduler.Implicits.global
import org.openjdk.jmh.annotations._

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisWriterBenchmark extends RedisBenchFixture {

  @Setup
  def setup(): Unit = {
    flushdb
    Redis.del((1 to maxKey).map(_.toString): _*).runSyncUnsafe()
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def hashWriter(): Unit = {
    val key = getNextKey.toString
    val field = getNextField.toString
    val value = getNextValue.toString
    RedisHash.hset(key, field, value).runSyncUnsafe()
  }

  @Benchmark
  def stringWriter(): Unit = {
    val key = getNextKey.toString
    val value = getNextValue.toString
    RedisString.set(key, value).runSyncUnsafe()
  }

  @Benchmark
  def listWriter(): Unit = {
    val key = getNextKey.toString
    val value = getNextValue.toString
    RedisList.rpush(key, value).runSyncUnsafe()
  }

  @Benchmark
  def setWriter(): Unit = {
    val key = getNextKey.toString
    val values = (1 to 3).map(_ => getNextValue.toString)
    RedisSet.sadd(key, values: _*).runSyncUnsafe()
  }

  @Benchmark
  def sortedSetWriter(): Unit = {
    val key = getNextKey.toString
    val value = getNextValue
    val scoredValue: ScoredValue[String] = ScoredValue.from(value.doubleValue, Optional.of(value.toString))
    RedisSortedSet.zadd(key, scoredValue).runSyncUnsafe()
  }

}
