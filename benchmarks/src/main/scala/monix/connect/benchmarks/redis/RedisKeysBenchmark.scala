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
import monix.connect.redis.{Redis, RedisKey}
import monix.execution.Scheduler.Implicits.global
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
    keysCycle = scala.Stream.continually(keys).flatten.iterator

    (1 to maxKey).foreach { key =>
      val value = key.toString
      val f = Redis.set(key.toString, value).runToFuture
      Await.ready(f, 1.seconds)
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def keyExistsReader(): Unit = {
    val f = RedisKey.exists(keysCycle.next).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def keyPttlReader(): Unit = {
    val f = RedisKey.pttl(keysCycle.next).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserDiscKeyExistsReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.exists(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserDiscKeyPttlReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.pttl(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousKeyExistsReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.exists[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousKeyPttlReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.pttl[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsKeyExistsReader(): Unit = {
    val f = redis4catsConn.use(c => c.exists(keysCycle.next)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsKeyPttlReader(): Unit = {
    val f = redis4catsConn.use(c => c.pttl(keysCycle.next)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
