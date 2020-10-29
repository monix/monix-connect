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

package monix.connect.benchmarks.redis.redis4cats

import dev.profunktor.redis4cats.effects.{Score, ScoreWithValue}
import org.openjdk.jmh.annotations._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
class Redis4CatsWriterBenchmark extends Redis4CatsBenchFixture {
  var keysCycle: Iterator[String] = _
  val keys = (0 to maxKey).toList.map(_.toString)

  @Setup
  def setup(): Unit = {
    flushdb
    keysCycle = Stream.continually(keys).flatten.iterator
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
    val f = redis4catsConn.use(c => c.hSet(key, field, value)).unsafeToFuture()
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def stringWriter(): Unit = {
    val key = keysCycle.next
    val value = getRandomString
    val f = redis4catsConn.use(c => c.set(key, value)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def listWriter(): Unit = {
    val key = keysCycle.next
    val value = getRandomString
    val f = redis4catsConn.use(c => c.rPush(key, value)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def setWriter(): Unit = {
    val key = keysCycle.next
    val values = (1 to 3).map(_ => getRandomString)
    val f = redis4catsConn.use(c => c.sAdd(key, values: _*)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }

  @Benchmark
  def sortedSetWriter(): Unit = {
    val key = keysCycle.next
    val value = getRandomString
    val scoredValue: ScoreWithValue[String] = ScoreWithValue(Score(rnd.nextDouble), value)
    val f = redis4catsConn.use(c => c.zAdd(key, args = None, scoredValue)).unsafeToFuture
    Await.ready(f, 1.seconds)
  }
}
