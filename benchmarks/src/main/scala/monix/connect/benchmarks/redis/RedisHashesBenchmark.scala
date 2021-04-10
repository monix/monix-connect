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
class RedisHashesBenchmark extends RedisBenchFixture {

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
            val field = key.toString
            cmd.hash.hSet(key.toString, field, value)
          }
        })
      .runSyncUnsafe()
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def monixHSet(): Unit = {
    val key = keysCycle.next
    val f = monixRedis.use(_.hash.hSet(key, key, key)).runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def monixHGetAll(): Unit = {
    val key = keysCycle.next
    val f = monixRedis.use(_.hash.hGetAll(key).lastOptionL).runToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserdiscHSet(): Unit = {
    val key = keysCycle.next
    val f = laserdConn
      .use(c => c.send(cmd.hset[String](Key.unsafeFrom(key), Key.unsafeFrom(key), key)))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def laserdiscHGetAll(): Unit = {
    val key = keysCycle.next
    val f = laserdConn.use(c => c.send(cmd.hgetall(Key.unsafeFrom(key)))).unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redicolousHSet(): Unit = {
    val key = keysCycle.next
    val f = redicolousConn
      .use(c => RedisCommands.hset[RedisIO](key, key, key).run(c))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redicolousHGetAll(): Unit = {
    val key = keysCycle.next
    val f = redicolousConn
      .use(c => RedisCommands.hgetall[RedisIO](key).run(c))
      .unsafeToFuture
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redis4catsHSet(): Unit = {
    val key = keysCycle.next
    val f = redis4catsConn.use(c => c.hSet(key, key, key)).unsafeToFuture()
    Await.ready(f, 2.seconds)
  }

  @Benchmark
  def redis4catsHGetAll(): Unit = {
    val key = keysCycle.next
    val f = redis4catsConn.use(c => c.hGetAll(key)).unsafeToFuture
    Await.ready(f, 2.seconds)
  }
}
