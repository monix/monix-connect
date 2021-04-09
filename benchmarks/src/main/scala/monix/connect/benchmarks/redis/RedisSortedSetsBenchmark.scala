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

import dev.profunktor.redis4cats.effects.{Score, ScoreWithValue, ZRange}
import monix.connect.redis.domain.{VScore, ZRange => MonixZrange}
import io.chrisdavenport.rediculous.RedisCommands
import laserdisc.fs2._
import laserdisc.protocol.SortedSetP.ScoreRange
import laserdisc.{Key, OneOrMore, ValidDouble, all => cmd}
import monix.eval.Task
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
  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = scala.Stream.continually(keys).flatten.iterator

    monixRedis
      .use(cmd =>
        Task.parSequence {
          (1 to maxKey).map { key =>
            val value = key.toString
            val scoredValue: VScore[String] = VScore(Option(value), key.toDouble)
            monixRedis.use(_.sortedSet.zAdd(key.toString, scoredValue))
          }
        })
      .runSyncUnsafe()
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def monixZAdd(): Unit = {
    val key = keysCycle.next
    val value = key
    val f = monixRedis.use(_.sortedSet.zAdd(key, VScore(value, key.toDouble))).runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def monixZCard(): Unit = {
    val f = monixRedis.use(_.sortedSet.zCard(keysCycle.next)).runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def monixZCount(): Unit = {
    monixRedis.use(_.sortedSet.zCount(keysCycle.next, MonixZrange(lowerBoundary, upperBoundary))).runSyncUnsafe()
  }

  @Benchmark
  def monixSortedZRangeByScore(): Unit = {
    val f = monixRedis
      .use(_.sortedSet.zRangeByScore(keysCycle.next, MonixZrange(lowerBoundary, upperBoundary)).lastOptionL)
      .runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserdiscZAdd(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val scoredMembers: OneOrMore[(String, ValidDouble)] =
      OneOrMore.unsafeFrom[(String, ValidDouble)](List((value, ValidDouble.unsafeFrom(key.toDouble))))
    val f = laserdConn
      .use(c => c.send(cmd.zadd(Key.unsafeFrom(key.toString), scoredMembers)))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserdiscZCard(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.zcard(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserdiscZCount(): Unit = {
    val range = ScoreRange.open(ValidDouble.unsafeFrom(lowerBoundary), ValidDouble.unsafeFrom(upperBoundary))
    val f = laserdConn
      .use(c => c.send(cmd.zcount(Key.unsafeFrom(keysCycle.next), range)))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserdiscZRangeByScore(): Unit = {
    val f = laserdConn
      .use(c =>
        c.send(
          cmd.zrangebyscore[String](
            Key.unsafeFrom(keysCycle.next),
            ScoreRange.closed(ValidDouble.unsafeFrom(lowerBoundary), ValidDouble.unsafeFrom(upperBoundary)))))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redicolousZAdd(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = redicolousConn
      .use(c => RedisCommands.zadd[RedisIO](key, List((key.toDouble, value))).run(c))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redicolousZCard(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.zcard[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redicolousZCount(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.zcount[RedisIO](keysCycle.next, lowerBoundary, upperBoundary).run(c))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redicolousZRangeByScore(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.zrangebyscore[RedisIO](keysCycle.next, lowerBoundary, upperBoundary).run(c))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redis4catsZAdd(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val scoredValue: ScoreWithValue[String] = ScoreWithValue(Score(key.toDouble), value)
    val f = redis4catsConn.use(c => c.zAdd(key, args = None, scoredValue)).unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redis4catsZCard(): Unit = {
    val f = redis4catsConn.use(c => c.zCard(keysCycle.next)).unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redis4catsZLexCount(): Unit = {
    val f = redis4catsConn
      .use(c => c.zLexCount(keysCycle.next, ZRange(lowerBoundary.toString, upperBoundary.toString)))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redis4catsZRangeByScore(): Unit = {
    val f = redis4catsConn
      .use(c => c.zRangeByScore(keysCycle.next, ZRange(lowerBoundary, upperBoundary), None))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }
}
