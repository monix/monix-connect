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
import laserdisc.{Key, OneOrMore, all => cmd}
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisLaserdiscSetsBenchmark extends RedisLaserdiscBenchFixture {
  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = scala.Stream.continually(keys).flatten.iterator

    (1 to maxKey).foreach { key =>
      val values = (1 to 3).map(_ => getRandomString).toList
      val preparedValues = OneOrMore.unsafeFrom[String](values)
      val f = laserdConn
        .use(c => c.send(cmd.sadd(Key.unsafeFrom(key.toString), preparedValues)))
        .unsafeToFuture
      Await.ready(f, 1.seconds)
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def setDiffWriter(): Unit = {
    val key1 = keysCycle.next
    val key2 = keysCycle.next
    val keyDest = keysCycle.next
    val f = laserdConn
      .use(c => c.send(cmd.sinterstore(Key.unsafeFrom(keyDest), Key.unsafeFrom(key1), Key.unsafeFrom(key2))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def setCardReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.scard(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def setMembersReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.smembers(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
