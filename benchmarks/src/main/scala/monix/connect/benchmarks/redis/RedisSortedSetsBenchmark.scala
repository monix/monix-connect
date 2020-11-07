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

import dev.profunktor.redis4cats.effects.{Score, ScoreWithValue, ZRange}
import io.chrisdavenport.rediculous.RedisCommands
import io.lettuce.core.ScoredValue
import laserdisc.fs2._
import laserdisc.protocol.SortedSetP.ScoreRange
import laserdisc.{Index, Key, OneOrMore, ValidDouble, all => cmd}
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

  val lowerBoundary = 0
  val upperBoundary = 1000
  val range = io.lettuce.core.Range.create[Integer](lowerBoundary, upperBoundary)
  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = scala.Stream.continually(keys).flatten.iterator

    (1 to maxKey).foreach { key =>
      val value = key.toString
      val scoredValue: ScoredValue[String] = ScoredValue.from(key.toDouble, Optional.of(value))
      val f = RedisSortedSet.zadd(key.toString, scoredValue).runToFuture
      Await.ready(f, 1.seconds)
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def sortedSetWriter(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val scoredValue: ScoredValue[String] = ScoredValue.from(key.toDouble, Optional.of(value))
    val f = RedisSortedSet.zadd(key, scoredValue).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def sortedSetCardReader(): Unit = {
    val f = RedisSortedSet.zcard(keysCycle.next).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def sortedSetCountReader(): Unit = {
    val f = RedisSortedSet.zcount(keysCycle.next, range).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def sortedSetRangeReader(): Unit = {
    val f = RedisSortedSet.zrange(keysCycle.next, lowerBoundary, upperBoundary).toListL.runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscSortedSetWriter(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val scoredMembers: OneOrMore[(String, ValidDouble)] =
      OneOrMore.unsafeFrom[(String, ValidDouble)](List((value, ValidDouble.unsafeFrom(key.toDouble))))
    val f = laserdConn
      .use(c => c.send(cmd.zadd(Key.unsafeFrom(key.toString), scoredMembers)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscSortedSetCardReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.zcard(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscSortedSetCountReader(): Unit = {
    val range = ScoreRange.open(ValidDouble.unsafeFrom(lowerBoundary), ValidDouble.unsafeFrom(upperBoundary))
    val f = laserdConn
      .use(c => c.send(cmd.zcount(Key.unsafeFrom(keysCycle.next), range)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscSortedSetRangeReader(): Unit = {
    val f = laserdConn
      .use(c =>
        c.send(
          cmd.zrange[String](
            Key.unsafeFrom(keysCycle.next),
            Index.unsafeFrom(lowerBoundary),
            Index.unsafeFrom(upperBoundary))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousSortedSetWriter(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = redicolousConn
      .use(c => RedisCommands.zadd[RedisIO](key, List((key.toDouble, value))).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousSortedSetCardReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.zcard[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousSortedSetCountReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.zcount[RedisIO](keysCycle.next, lowerBoundary, upperBoundary).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousSortedSetRangeReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.zrange[RedisIO](keysCycle.next, lowerBoundary, upperBoundary).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsSortedSetWriter(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val scoredValue: ScoreWithValue[String] = ScoreWithValue(Score(key.toDouble), value)
    val f = redis4catsConn.use(c => c.zAdd(key, args = None, scoredValue)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsSortedSetCardReader(): Unit = {
    val f = redis4catsConn.use(c => c.zCard(keysCycle.next)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsSortedSetCountReader(): Unit = {
    val f = redis4catsConn
      .use(c => c.zLexCount(keysCycle.next, ZRange(lowerBoundary.toString, upperBoundary.toString)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsSortedSetRangeReader(): Unit = {
    val f = redis4catsConn.use(c => c.zRange(keysCycle.next, lowerBoundary, upperBoundary)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
