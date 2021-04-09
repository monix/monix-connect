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
import laserdisc.{Key, all => cmd}
import monix.eval.Task
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisKeysBenchmark extends RedisBenchFixture {

  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = LazyList.continually(keys).flatten.iterator
    monixRedis
      .use(cmd =>
        Task.parSequence {
          (1 to maxKey).map { key =>
            val value = key.toString
            cmd.string.set(key.toString, value)
          }
        })
      .runSyncUnsafe()
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def monixKeyExists(): Unit = {
    val f = monixRedis.use(_.key.exists(keysCycle.next)).runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def monixKeyTtl(): Unit = {
    val f = monixRedis.use(_.key.pttl(keysCycle.next)).runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserDiscKeyExists(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.exists(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserDiscKeyPttl(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.pttl(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }
  @Benchmark
  def redicolousKeyExists(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.exists[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redicolousKeyPttl(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.pttl[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redis4catsKeyExists(): Unit = {
    val f = redis4catsConn.use(c => c.exists(keysCycle.next)).unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redis4catsKeyPttl(): Unit = {
    val f = redis4catsConn.use(c => c.pttl(keysCycle.next)).unsafeToFuture
    Await.ready(f, 2.seconds)
  }

}
