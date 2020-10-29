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

import io.lettuce.core.{Range, ScoredValue}
import monix.connect.redis.RedisSortedSet
import monix.execution.Scheduler.Implicits.global
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisSortedSetsBenchmark extends RedisBenchFixture {

  val unboundedRange = Range.unbounded()
  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = Stream.continually(keys).flatten.iterator

    (1 to maxKey).foreach { key =>
      val value = getRandomString
      val scoredValue: ScoredValue[String] = ScoredValue.from(rnd.nextDouble, Optional.of(value))
      val f = RedisSortedSet.zadd(key.toString, scoredValue).runToFuture
      Await.ready(f, 1.seconds)
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def sortedSetCardReader(): Unit = {
    val f = RedisSortedSet.zcard(keysCycle.next).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def sortedSetCountReader(): Unit = {
    val f = RedisSortedSet.zcount(keysCycle.next, unboundedRange).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def sortedSetRangeReader(): Unit = {
    val f = RedisSortedSet.zrange(keysCycle.next, 0, 1).toListL.runToFuture
    Await.ready(f, 1.seconds)
  }
}
