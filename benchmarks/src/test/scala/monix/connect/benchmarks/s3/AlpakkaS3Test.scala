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

package monix.connect.benchmarks.s3

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.s3.ObjectMetadata
import akka.stream.alpakka.s3.scaladsl.{S3 => AkkaS3}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import monix.connect.s3.S3
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class AlpakkaS3Test extends AnyFlatSpec with Matchers with S3MonixFixture {

  val bucket = "sample-bucket"
  val s3Key = "sample-key"

  implicit val actorSystem = ActorSystem("s3-benchmark")

  implicit val s = Scheduler.io("io")

  Try(s3Resource.use(_.createBucket(bucket)).runSyncUnsafe()) match {
    case Success(_) => println(s"Created S3 bucket ${bucket} ")
    case Failure(e) => println(s"Failed to create S3 bucket ${bucket} with exception: ${e.getMessage}")
  }

  "Localstack" should "be reached from alpakka s3" in {
    val ob: Observable[Array[Byte]] = Observable.now("awldawfafafgafwa".getBytes())
    Await.result(s3Resource.use(s3 => ob.consumeWith(s3.uploadMultipart(bucket, s3Key))).runToFuture(s), Duration.Inf)

    val f = AkkaS3
      .download(bucket, s3Key)
      .via {
        Flow[Option[(Source[ByteString, NotUsed], ObjectMetadata)]].map {
          case Some(source) => source._1.runWith(Sink.last)
        }
      }
      .toMat(Sink.last)(Keep.right)
      .run()

    Await.result(f, Duration.Inf)
  }

  it should "be reached from monix" in {
    println("Buckets: " + S3.create().use(_.listBuckets().toListL).runSyncUnsafe())
  }
}