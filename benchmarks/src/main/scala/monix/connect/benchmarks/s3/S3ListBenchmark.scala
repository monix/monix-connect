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

package monix.connect.benchmarks.s3

import akka.actor.ActorSystem
import akka.stream.alpakka.s3.scaladsl.{S3 => AkkaS3}
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.{Keep, Source}
import akka.util.ByteString
import monix.connect.s3.S3
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.openjdk.jmh.annotations._
import org.scalacheck.Gen

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 3)
@Warmup(iterations = 1)
@Fork(1)
@Threads(3)
class S3ListBenchmark extends S3MonixFixture {

  implicit val actorSystem = ActorSystem("s3-list-benchmark")
  var size: Int = 20
  implicit val s = Scheduler.io("s3-list-benchmark")
  val bucketName = "list-benchmark"
  var content: Array[Byte] = nonEmptyString.value().getBytes()
  val prefix = s"list-benchmark/${nonEmptyString.value()}/"
  val keys: List[String] = Gen.listOfN(size, Gen.nonEmptyListOf(Gen.alphaChar).map(l => prefix + l.mkString)).sample.get

  @Setup
  def setup(): Unit = {
    createBucket(bucketName)
    s3Resource.use { s3 =>
      Task
        .sequence(keys.map { key => s3.upload(bucketName, key, content) })
    }.runSyncUnsafe()
    ()
  }

  @TearDown
  def tearDown(): Unit = {
    actorSystem.terminate()
  }

  @Benchmark
  def monixConfigListObjects(): Unit = {
    val t = S3.fromConfig.use(_.listObjects(bucketName, Some(prefix)).lastL)

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixListObjects(): Unit = {
    val t = s3Resource.use(_.listObjects(bucketName, Some(prefix)).lastL)

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixUnsafeListObjects(): Unit = {
    val t = S3.createUnsafe(s3AsyncClient).listObjects(bucketName, Some(prefix)).lastL

    Await.result(t.runToFuture(s), Duration.Inf)
  }
  @Benchmark
  def akkaListObjects(): Unit = {
    val f = AkkaS3.listBucket(bucketName, prefix = Some(prefix)).toMat(Sink.last)(Keep.right).run()

    Await.result(f, Duration.Inf)
  }
}
