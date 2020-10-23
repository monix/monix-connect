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

package monix.connect.s3

import java.io.{File, FileInputStream}
import java.nio.file.Files

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class MultipartUploadSubscriberSuite
  extends AnyFlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"
  private val s3Resource = S3.create(staticCredProvider, Region.AWS_GLOBAL, Some(minioEndPoint), Some(httpClient))

  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(s3Resource.use(_.createBucket(bucketName)).runSyncUnsafe()) match {
      case Success(_) => info(s"Created s3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create s3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }

  "MultipartUploadSubscriber" should "upload multipart when only a single chunk was emitted" in {
    //given
    val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
    val content: Array[Byte] = Gen.alphaUpperStr.sample.get.getBytes

    //when
    val t = s3Resource.use { s3 => Observable.pure(content).consumeWith(s3.uploadMultipart(bucketName, key)) }

    //then
    whenReady(t.runToFuture) { completeMultipartUpload =>
      val s3Object: Array[Byte] = download(bucketName, key).get
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe content
    }
  }

  it should "upload multipart chunks (of less than minimum size) are passed" in {
    //given
    val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
    val chunks: List[Array[Byte]] = Gen.listOfN(10, Gen.alphaUpperStr).map(_.map(_.getBytes)).sample.get

    //when
    val t = s3Resource.use { s3 => Observable.fromIterable(chunks).consumeWith(s3.uploadMultipart(bucketName, key)) }

    //then
    whenReady(t.runToFuture) { response =>
      eventually {
        val s3Object: Array[Byte] = download(bucketName, key).get
        s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
        response shouldBe a[CompleteMultipartUploadResponse]
        s3Object shouldBe chunks.flatten
      }
    }
  }

  it should "upload multipart single chunk of size (1MB)" in {
    //given
    val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
    val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
    val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)

    //when
    val t = s3Resource.use { s3 =>
      val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
        s3.uploadMultipart(bucketName, key)
      ob.consumeWith(consumer)
    }

    //then
    val expectedArrayByte = ob.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL.runSyncUnsafe()
    whenReady(t.runToFuture) { completeMultipartUpload =>
      val s3Object: Array[Byte] = download(bucketName, key).get
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe expectedArrayByte
    }
  }

  it should "upload one part emitted with chunks smaller than the min size" in {
    //given
    val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
    val chunk = Files.readAllBytes(new File(resourceFile("test.csv")).toPath) //test.csv ~= 244Kb
    val chunks = Array.fill(24)(chunk) // at the 21th chunk (5MB) the upload should be triggered

    //when
    val response = s3Resource.use { s3 =>
      Observable.fromIterable(chunks).consumeWith(s3.uploadMultipart(bucketName, key))
    }.runSyncUnsafe()

    //then
    eventually {
      val s3Object: Array[Byte] = download(bucketName, key).get
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      response shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe chunks.flatten
    }
  }

  it should "upload two parts emitted with chunks smaller than the min size" in {
    //given
    val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
    val chunk = Files.readAllBytes(new File(resourceFile("test.csv")).toPath) //test.csv ~= 244Kb
    val chunks = Array.fill(48)(chunk) // two part uploads will be triggered at th 21th and 42th chunks

    //when
    val response = s3Resource.use { s3 =>
      Observable.fromIterable(chunks).consumeWith(s3.uploadMultipart(bucketName, key))
    }.runSyncUnsafe()

    //then
    eventually {
      val s3Object: Array[Byte] = download(bucketName, key).get
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      response shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe chunks.flatten
    }
  }

  it should " upload big chunks" in {
    //given
    val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
    val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
    val ob: Observable[Array[Byte]] = Observable
      .fromInputStream(inputStream)
      .foldLeft(Array.emptyByteArray)((acc, chunk) => acc ++ chunk ++ chunk ++ chunk ++ chunk ++ chunk) //duplicates each chunk * 5

    //when
    val response = s3Resource.use { s3 =>
      ob.consumeWith(s3.uploadMultipart(bucketName, key))
    }.runSyncUnsafe()

    //then
    val expectedArrayByte = ob.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL.runSyncUnsafe()
    eventually {
      val s3Object: Array[Byte] = download(bucketName, key).get
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      response shouldBe a[CompleteMultipartUploadResponse]
      s3Object shouldBe expectedArrayByte
    }
  }

  it should "fails to upload in multipart when min chunk size is lower than (5MB)" in {
    //given
    val minChunkSize = 5242879 //minimum is 5242880

    //when
    val f = s3Resource.use { s3 =>
      Observable.now(Array.emptyByteArray)
        .consumeWith(s3.uploadMultipart("no-bucket", "no-key", minChunkSize = minChunkSize))
    }.runToFuture

    //then
    f.value.get shouldBe a[Failure[IllegalArgumentException]]
  }

}
