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
import monix.connect.redis.RedisString
import monix.execution.Scheduler.Implicits.global
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisStringsBenchmark extends RedisBenchFixture {

  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = scala.Stream.continually(keys).flatten.iterator

    (1 to maxKey).foreach { key =>
      val value = key.toString
      val f = RedisString.set(key.toString, value).runToFuture
      Await.ready(f, 1.seconds)
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def stringWriter(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = RedisString.set(key, value).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def stringAppender(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = RedisString.append(key, value).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def stringReader(): Unit = {
    val f = RedisString.get(keysCycle.next).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscStringWriter(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = laserdConn
      .use(c => c.send(cmd.set[String](Key.unsafeFrom(key), value)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscStringAppender(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = laserdConn
      .use(c => c.send(cmd.append[String](Key.unsafeFrom(key), value)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscStringReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.get[String](Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousStringWriter(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = redicolousConn
      .use(c => RedisCommands.set[RedisIO](key, value).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousStringAppender(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = redicolousConn
      .use(c => RedisCommands.append[RedisIO](key, value).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousStringReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.get[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsStringWriter(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = redis4catsConn.use(c => c.set(key, value)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsStringAppender(): Unit = {
    val key = keysCycle.next
    val value = key.toString
    val f = redis4catsConn.use(c => c.append(key, value)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsStringReader(): Unit = {
    val f = redis4catsConn.use(c => c.get(keysCycle.next)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
