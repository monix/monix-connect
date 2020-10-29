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

package monix.connect.benchmarks.redis.laserdisc

import cats.implicits._
import laserdisc.fs2._
import laserdisc.{Key, OneOrMore, ValidDouble, all => cmd}
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisLaserdiscWriterBenchmark extends RedisLaserdiscBenchFixture {
  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = scala.Stream.continually(keys).flatten.iterator
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def hashWriter(): Unit = {
    val key = keysCycle.next
    val field = getRandomString
    val value = getRandomString
    val f = laserdConn
      .use(c => c.send(cmd.hset[String](Key.unsafeFrom(key.toString), Key.unsafeFrom(field), value)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def stringWriter(): Unit = {
    val key = keysCycle.next
    val value = getRandomString
    val f = laserdConn
      .use(c => c.send(cmd.set[String](Key.unsafeFrom(key), value)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def listWriter(): Unit = {
    val key = keysCycle.next
    val value = getRandomString
    val f = laserdConn
      .use(c => c.send(cmd.rpush(Key.unsafeFrom(key.toString), value)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def setWriter(): Unit = {
    val key = keysCycle.next
    val values = (1 to 3).map(_ => getRandomString).toList
    val preparedValues = OneOrMore.unsafeFrom[String](values)
    val f = laserdConn
      .use(c => c.send(cmd.sadd(Key.unsafeFrom(key.toString), preparedValues)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def sortedSetWriter(): Unit = {
    val key = keysCycle.next
    val value = getRandomString
    val scoredMembers: OneOrMore[(String, ValidDouble)] =
      OneOrMore.unsafeFrom[(String, ValidDouble)](List((value, ValidDouble.unsafeFrom(rnd.nextDouble))))
    val f = laserdConn
      .use(c => c.send(cmd.zadd(Key.unsafeFrom(key.toString), scoredMembers)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
