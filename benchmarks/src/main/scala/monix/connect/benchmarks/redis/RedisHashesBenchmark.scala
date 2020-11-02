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

import io.chrisdavenport.rediculous.RedisCommands
import laserdisc.fs2._
import laserdisc.{Key, all => cmd}
import monix.connect.redis.RedisHash
import monix.execution.Scheduler.Implicits.global
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

    (1 to maxKey).foreach { key =>
      val value = key.toString
      val field = key.toString
      val f = RedisHash.hset(key.toString, field, value).runToFuture
      Await.ready(f, 1.seconds)
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def hashWriter(): Unit = {
    val key = keysCycle.next
    val f = RedisHash.hset(key, key, key).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def hashFieldValueReader(): Unit = {
    val key = keysCycle.next
    val f = RedisHash.hget(key, key).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def hashAllReader(): Unit = {
    val key = keysCycle.next
    val f = RedisHash.hgetall(key).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscHashWriter(): Unit = {
    val key = keysCycle.next
    val f = laserdConn
      .use(c => c.send(cmd.hset[String](Key.unsafeFrom(key), Key.unsafeFrom(key), key)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscHashFieldValueReader(): Unit = {
    val key = keysCycle.next
    val f = laserdConn
      .use(c => c.send(cmd.hget[String](Key.unsafeFrom(key), Key.unsafeFrom(key))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscHashAllReader(): Unit = {
    val key = keysCycle.next
    val f = laserdConn.use(c => c.send(cmd.hgetall(Key.unsafeFrom(key)))).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousHashWriter(): Unit = {
    val key = keysCycle.next
    val f = redicolousConn
      .use(c => RedisCommands.hset[RedisIO](key, key, key).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousHashFieldValueReader(): Unit = {
    val key = keysCycle.next
    val f = redicolousConn
      .use(c => RedisCommands.hget[RedisIO](key, key).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousHashAllReader(): Unit = {
    val key = keysCycle.next
    val f = redicolousConn
      .use(c => RedisCommands.hgetall[RedisIO](key).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsHashWriter(): Unit = {
    val key = keysCycle.next
    val f = redis4catsConn.use(c => c.hSet(key, key, key)).unsafeToFuture()
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsHashFieldValueReader(): Unit = {
    val key = keysCycle.next
    val f = redis4catsConn.use(c => c.hGet(key, key)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsHashAllReader(): Unit = {
    val key = keysCycle.next
    val f = redis4catsConn.use(c => c.hGetAll(key)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
