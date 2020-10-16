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
import akka.stream.scaladsl.{Keep, Source}
import akka.util.ByteString
import monix.connect.s3.S3

import scala.concurrent.Await
import scala.concurrent.duration.Duration

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 3)
@Warmup(iterations = 1)
@Fork(1)
@Threads(3)
class S3UploadBenchmark extends S3MonixFixture {

  implicit val actorSystem = ActorSystem("s3-upload-benchmark")
  var size: Int = 20
  implicit val s = Scheduler.io("s3-upload-benchmark")
  val bucketName = "s3-upload-benchmarks"
  var contents: Array[Array[Byte]] = Array.fill(size)(nonEmptyString.value().getBytes())

  @Setup
  def setup(): Unit = {
    createBucket(bucketName)
    ()
  }

  @TearDown
  def tearDown(): Unit = {
    actorSystem.terminate()
  }

  @Benchmark
  def monixUpload(): Unit = {
    val newKey = nonEmptyString.value()
    val t = s3Resource.use(_.upload(bucketName, newKey, contents.flatten))

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixConfigUpload(): Unit = {
    val newKey = nonEmptyString.value()
    val t = S3.create().use(_.upload(bucketName, newKey, contents.flatten))

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixUnsafeUpload(): Unit = {
    val newKey = nonEmptyString.value()
    val t = S3.createUnsafe(s3AsyncClient).upload(bucketName, newKey, contents.flatten)

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixUploadMultipart(): Unit = {
    val newKey = nonEmptyString.value()
    val t = s3Resource.use(s3 => Observable.fromIterable(contents).consumeWith(s3.uploadMultipart(bucketName, newKey)))

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixConfigUploadMultipart(): Unit = {
    val newKey = nonEmptyString.value()
    val t = S3.create().use(s3 => Observable.fromIterable(contents).consumeWith(s3.uploadMultipart(bucketName, newKey)))

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def monixUnsafeUploadMultipart(): Unit = {
    val newKey = nonEmptyString.value()

    val t =
      Observable
        .fromIterable(contents)
        .consumeWith(S3.createUnsafe(s3AsyncClient).uploadMultipart(bucketName, newKey))

    Await.result(t.runToFuture(s), Duration.Inf)
  }

  @Benchmark
  def akkaUploadMultipart(): Unit = {
    val newKey = nonEmptyString.value()

    val f = Source
      .fromIterator(() => contents.map(ByteString.fromArray).iterator)
      .toMat(AkkaS3.multipartUpload(bucketName, newKey))(Keep.right)
      .run()

    Await.result(f, Duration.Inf)
  }
}
