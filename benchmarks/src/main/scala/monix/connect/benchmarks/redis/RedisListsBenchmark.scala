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
import laserdisc.{Index, Key, all => cmd}
import monix.execution.Scheduler.Implicits.global
import org.openjdk.jmh.annotations._
import dev.profunktor.redis4cats.data.RedisCodec
import dev.profunktor.redis4cats.codecs.splits.stringDoubleEpi
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class RedisListsBenchmark extends RedisBenchFixture {

  val lowerBoundary = 0
  val upperBoundary = 1000
  var keysCycle: Iterator[String] = _

  @Setup
  def setup(): Unit = {
    flushdb

    val keys = (0 to maxKey).toList.map(_.toString)
    keysCycle = scala.Stream.continually(keys).flatten.iterator

    (1 to maxKey).foreach { key =>
      val value = key.toString
      val f = monixRedis.use(_.list.rPush(key.toString, value)).runToFuture
      Await.ready(f, 1.seconds)
    }
  }

  @TearDown
  def shutdown(): Unit = {
    flushdb
  }

  @Benchmark
  def listWriter(): Unit = {
    val key = keysCycle.next
    val value = key
    val f = monixRedis.use(_.list.rPush(key, value)).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def listLengthReader(): Unit = {
    val f = monixRedis.use(_.list.lLen(keysCycle.next)).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def listByIndexReader(): Unit = {
    val f = monixRedis.use(_.list.lIndex(keysCycle.next, 0)).runToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def listRangeReader(): Unit = {
    val f = monixRedis.use(_.list.lRange(keysCycle.next, lowerBoundary, upperBoundary).toListL).runToFuture
    Await.ready(f, 1.seconds)
  }
  /*
  @Benchmark
  def laserdiscListWriter(): Unit = {
    val key = keysCycle.next
    val value = key
    val f = laserdConn
      .use(c => c.send(cmd.rpush[String](Key.unsafeFrom(key), value)))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscListLengthReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.llen(Key.unsafeFrom(keysCycle.next))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscListByIndexReader(): Unit = {
    val f = laserdConn
      .use(c => c.send(cmd.lindex[String](Key.unsafeFrom(keysCycle.next), Index.unsafeFrom(0))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def laserdiscListRangeReader(): Unit = {
    val f = laserdConn
      .use(c =>
        c.send(
          cmd.lrange[String](
            Key.unsafeFrom(keysCycle.next),
            Index.unsafeFrom(lowerBoundary),
            Index.unsafeFrom(upperBoundary))))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousListWriter(): Unit = {
    val key = keysCycle.next
    val value = List(key)
    val f = redicolousConn
      .use(c => RedisCommands.rpush[RedisIO](key, value).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousListLengthReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.llen[RedisIO](keysCycle.next).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousListByIndexReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.lindex[RedisIO](keysCycle.next, 0).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redicolousListRangeReader(): Unit = {
    val f = redicolousConn
      .use(c => RedisCommands.lrange[RedisIO](keysCycle.next, lowerBoundary, upperBoundary).run(c))
      .unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsListWriter(): Unit = {
    val key = keysCycle.next
    val value = key
    val f = redis4catsConn.use(c => c.rPush(key, value)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsListLengthReader(): Unit = {
    val f = redis4catsConn.use(c => c.lLen(keysCycle.next)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsListByIndexReader(): Unit = {
    val f = redis4catsConn.use(c => c.lIndex(keysCycle.next, 0)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def redis4catsListRangeReader(): Unit = {
    val f = redis4catsConn.use(c => c.lRange(keysCycle.next, lowerBoundary, upperBoundary)).unsafeToFuture
    Await.ready(f, 1.seconds)
  } */
}
