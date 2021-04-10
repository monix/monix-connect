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

import io.chrisdavenport.rediculous.RedisCommands
import laserdisc.fs2._
import laserdisc.{Key, OneOrMore, all => cmd}
import monix.eval.Task
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisSetsBenchmark extends RedisBenchFixture {

  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = scala.Stream.continually(keys).flatten.iterator

    monixRedis
      .use(cmd =>
        Task.sequence {
          (1 to maxKey).map { key =>
            val values = (1 to 3).map(i => (key + i).toString)
            cmd.set.sAdd(key.toString, values: _*)
          }
        })
      .runSyncUnsafe()
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def monixSAdd(): Unit = {
    val key = keysCycle.next
    val values = (1 to 3).map(i => (key + i))
    val f = monixRedis.use(_.set.sAdd(key, values: _*)).runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def monixSMembers(): Unit = {
    val f = monixRedis.use(_.set.sMembers(keysCycle.next).lastOptionL).runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserdiscSAdd(): Unit = {
    val key = keysCycle.next
    val values = (1 to 3).map(i => (key + i).toString).toList
    val preparedValues = OneOrMore.unsafeFrom[String](values)
    val f = laserdConn
      .use(c => c.send(cmd.sadd(Key.unsafeFrom(key.toString), preparedValues)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }
  @Benchmark
  def laserdiscSMembers(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.smembers(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousSAdd(): Unit = {
    val key = keysCycle.next
    val values = (1 to 3).map(i => (key + i)).toList
    val f = redicolousConn
      .use(c => RedisCommands.sadd[RedisIO](key, values).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousSMembers(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.smembers[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsSAdd(): Unit = {
    val key = keysCycle.next
    val values = (1 to 3).map(i => (key + i))
    val f = redis4catsConn.use(c => c.sAdd(key, values: _*)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsSMembers(): Unit = {
    val f = redis4catsConn.use(c => c.sMembers(keysCycle.next)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
