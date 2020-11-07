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

import java.io.FileInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.s3.ObjectMetadata
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.openjdk.jmh.annotations.{
  Benchmark,
  BenchmarkMode,
  Fork,
  Measurement,
  Mode,
  Scope,
  Setup,
  State,
  TearDown,
  Threads,
  Warmup
}
import akka.stream.alpakka.s3.scaladsl.{S3 => AkkaS3}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import monix.connect.s3.S3

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 3)
@Warmup(iterations = 1)
@Fork(1)
@Threads(3)
class S3DownloadBenchmark extends S3MonixFixture {

  implicit val actorSystem = ActorSystem("s3-download-benchmark")
  var size: Int = 250
  implicit val s = Scheduler.io("s3-download-benchmark")
  val bucketName = "s3-upload-benchmark"
  var key: String = _

  @Setup
  def setup(): Unit = {
    key = nonEmptyString.value()
    createBucket(bucketName)
    val inputStream = Task(new FileInputStream("./src/main/resources/test.csv"))
    val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
    val f = s3Resource.use(s3 => ob.consumeWith(s3.uploadMultipart(bucketName, key))).runToFuture(s)
    Await.result(f, Duration.Inf)
    ()
  }

  @TearDown
  def tearDown(): Unit = {
    actorSystem.terminate()
  }

  @Benchmark
  def monixDownload(): Unit = {
    val t = s3Resource.use(_.download(bucketName, key))

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  def monixConfigDownload(): Unit = {
    val t = S3.fromConfig.use(_.download(bucketName, key))

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixUnsafeDownload(): Unit = {
    val t = S3.createUnsafe(s3AsyncClient).download(bucketName, key)

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def akkaDownload(): Unit = {

    //given
    val f = AkkaS3
      .download(bucketName, key)
      .via {
        Flow[Option[(Source[ByteString, NotUsed], ObjectMetadata)]].map {
          case Some(source) => source._1.runWith(Sink.last)
        }
      }
      .toMat(Sink.last)(Keep.right)
      .run()

    Await.result(f, Duration.Inf)
  }

  @Benchmark
  def monixDownloadMultipart(): Unit = {
    val t = s3Resource.use(_.downloadMultipart(bucketName, key, 5242880).lastL)

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixConfigDownloadMultipart(): Unit = {
    val t = S3.fromConfig.use(_.downloadMultipart(bucketName, key, 5242880).lastL)

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixUnsafeDownloadMultipart(): Unit = {
    val t = S3.createUnsafe(s3AsyncClient).downloadMultipart(bucketName, key, 5242880).lastL

    Await.result(t.runToFuture(s), Duration.Inf)
  }

}
