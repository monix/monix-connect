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

package monix.connect.s3

import java.io.{File, FileInputStream}
import java.nio.file.Files
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class MultipartUploadSubscriberSuite
  extends AsyncFlatSpec with Matchers with MonixTaskSpec with BeforeAndAfterAll with S3Fixture {

  private val bucketName = "multipart-upload-test-bucket"
  override implicit val scheduler = Scheduler.io("multipart-download-observable-suite")

  override def beforeAll(): Unit = {
    super.beforeAll()
    unsafeS3.createBucket(bucketName).attempt.runSyncUnsafe() match {
      case Right(_) => info(s"Created s3 bucket $bucketName ")
      case Left(e) => info(s"Failed to create s3 bucket $bucketName with exception: ${e.getMessage}")
    }
  }

  "MultipartUploadSubscriber" should "upload multipart when only a single chunk was emitted" in {
    val key = genKey.sample.get
    val content: Array[Byte] = Gen.alphaUpperStr.sample.get.getBytes

    for {
      completeMultipartUpload <- Observable.pure(content).consumeWith(unsafeS3.uploadMultipart(bucketName, key))
      s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
      existsObject <- unsafeS3.existsObject(bucketName, key)
    } yield {
      existsObject shouldBe true
      completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe content
    }
  }

  it can "upload chunks (of less than minimum size) are passed" in {
    val key: String = genKey.sample.get
    val chunks: List[Array[Byte]] = Gen.listOfN(10, Gen.alphaUpperStr).map(_.map(_.getBytes)).sample.get

    for {
      completeMultipartUpload <- Observable.fromIterable(chunks).consumeWith(unsafeS3.uploadMultipart(bucketName, key))
      s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
      existsObject <- unsafeS3.existsObject(bucketName, key)
    } yield {
      existsObject shouldBe true
      completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe chunks.flatten
    }
  }

  it should "upload in a single chunk of size (1MB)" in {
    //given
    val key = genKey.sample.get
    val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
    val fileData = Observable.fromInputStream(inputStream)
    for {
      completeMultipartUpload <- fileData.consumeWith(unsafeS3.uploadMultipart(bucketName, key))
      s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
      existsObject <- unsafeS3.existsObject(bucketName, key)
      expectedArrayByte <- fileData.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL
    } yield {
      existsObject shouldBe true
      completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe expectedArrayByte
    }
  }

  it should "aggregate and upload chunks emitted from smaller size than the minimum" in {
    //given
    val key = genKey.sample.get
    val chunk = Files.readAllBytes(new File(resourceFile("244KB.csv")).toPath) //test.csv ~= 244Kb
    val chunks = Array.fill(24)(chunk) // at the 21th chunk (5MB) the upload should be triggered

    //when
    for {
      completeMultipartUpload <- Observable.fromIterable(chunks).consumeWith(unsafeS3.uploadMultipart(bucketName, key))
     s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
      existsObject <- unsafeS3.existsObject(bucketName, key)
    } yield {
      completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe chunks.flatten
      existsObject shouldBe true
    }
  }

  it should "upload in two (aggregated) parts emitted from chunks smaller than the min size" in {
    val key = genKey.sample.get
    val chunk = Files.readAllBytes(new File(resourceFile("244KB.csv")).toPath) //test.csv ~= 244Kb
    val chunks = Array.fill(48)(chunk) // two part uploads will be triggered at th 21th and 42th chunks

    for {
       completeMultipartUpload <- Observable.fromIterable(chunks).consumeWith(unsafeS3.uploadMultipart(bucketName, key))
       existsObject <- unsafeS3.existsObject(bucketName, key)
       s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
     } yield {
       completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
       s3Object shouldBe chunks.flatten
       existsObject shouldBe true
     }

  }

  it should "upload big chunks" in {
    val key = genKey.sample.get
    val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
    val testData: Observable[Array[Byte]] = Observable
      .fromInputStream(inputStream)
      .foldLeft(Array.emptyByteArray)((acc, chunk) => acc ++ chunk ++ chunk ++ chunk ++ chunk ++ chunk) //each chunk * 5

    for {
      completeMultipartUpload <- testData.consumeWith(unsafeS3.uploadMultipart(bucketName, key))
      expectedArrayByte <- testData.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL
      s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
      existsObject <- unsafeS3.existsObject(bucketName, key)
    } yield {
      completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe expectedArrayByte
      existsObject shouldBe true
    }
  }

  it should "fail to upload in multipart when min chunk size is lower than (5MB)" in {
    val minChunkSize = 5242879 //minimum is 5242880

   Observable.now(Array.emptyByteArray)
     .consumeWith(unsafeS3.uploadMultipart("no-bucket", "no-key", minChunkSize = minChunkSize))
     .assertThrows[IllegalArgumentException]
  }

}
