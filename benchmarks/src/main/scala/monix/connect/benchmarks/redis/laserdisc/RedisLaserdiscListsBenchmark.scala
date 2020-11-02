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
import laserdisc.{Index, Key, all => cmd}
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisLaserdiscListsBenchmark extends RedisLaserdiscBenchFixture {
  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = scala.Stream.continually(keys).flatten.iterator

    (1 to maxKey).foreach { key =>
      val value = getRandomString
      val f = laserdConn
        .use(c => c.send(cmd.rpush(Key.unsafeFrom(key.toString), value)))
        .unsafeToFuture
      Await.ready(f, 1.seconds)
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def listLengthReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.llen(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def listByIndexReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.lindex[String](Key.unsafeFrom(keysCycle.next), Index.unsafeFrom(0))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def listRangeReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.lrange[String](Key.unsafeFrom(keysCycle.next), Index.unsafeFrom(0), Index.unsafeFrom(1))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
