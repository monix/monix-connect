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

import java.io.FileInputStream
import java.lang.Thread.sleep

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadResponse,
  NoSuchBucketException,
  NoSuchKeyException
}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class MultipartDownloadObservableSuite
  extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"
  private val s3Resource = S3.createWith(staticCredProvider, Region.AWS_GLOBAL, Some(minioEndPoint), Some(httpClient))

  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(s3Resource.use(_.createBucket(bucketName)).runSyncUnsafe()) match {
      case Success(_) => info(s"Created s3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create s3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }

  "MultipartDownloadObservable" should {

    "download a s3 object as byte array" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = Gen.alphaUpperStr.sample.get
      s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()

      //when
      val t: Task[Array[Byte]] = s3Resource.use(_.download(bucketName, key))

      //then
      whenReady(t.runToFuture) { actualContent: Array[Byte] =>
        s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
        actualContent shouldBe a[Array[Byte]]
        actualContent shouldBe content.getBytes()
      }
    }

    "download a s3 object bigger than 1MB as byte array" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
      val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
      s3Resource.use { s3 =>
        val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
          s3.uploadMultipart(bucketName, key)
        ob.consumeWith(consumer)
      }.runSyncUnsafe()

      //when
      val t = s3Resource.use(_.download(bucketName, key))

      //then
      whenReady(t.runToFuture) { actualContent: Array[Byte] =>
        val expectedArrayByte = ob.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).runSyncUnsafe()
        s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
        actualContent shouldBe a[Array[Byte]]
        actualContent.size shouldBe expectedArrayByte.size
        actualContent shouldBe expectedArrayByte
      }
    }

    "download the first n bytes form an object" in {
      //given
      val n = 5
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()

      //when
      val t: Task[Array[Byte]] = s3Resource.use(_.download(bucketName, key, Some(n)))

      //then
      whenReady(t.runToFuture) { partialContent: Array[Byte] =>
        s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
        partialContent shouldBe a[Array[Byte]]
        partialContent shouldBe content.getBytes().take(n)
      }
    }

    "download fails if the numberOfBytes is negative" in {
      //given
      val negativeNum = Gen.chooseNum(-10, -1).sample.get
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString

      //when
      val f = s3Resource.use(_.download(bucketName, key, Some(negativeNum))).runToFuture

      //then
      Await.ready(f, 3.seconds)
      f.value.get.isFailure shouldBe true
    }

    "download from a non existing key returns failed task" in {
      //given
      val key: String = "non-existing-key"

      //when
      val f: Future[Array[Byte]] = s3Resource.use(_.download(bucketName, key)).runToFuture(global)
      sleep(400)

      //then
      f.value.get shouldBe a[Failure[NoSuchKeyException]]
    }

    "downloadMultipart of small chunk size" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = nonEmptyString.value()
      s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()
      println("1!")

      //when
      val actualContent: Array[Byte] =
        s3Resource.use(_.downloadMultipart(bucketName, key, 2).toListL.map(_.flatten.toArray)).runSyncUnsafe()

      //then
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      actualContent shouldBe a[Array[Byte]]
      actualContent shouldBe content.getBytes()
    }

    "downloadMultipart in one part" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = nonEmptyString.value()
      s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()

      //when
      val actualContent: Array[Byte] =
        s3Resource.use(_.downloadMultipart(bucketName, key, 52428).toListL).map(_.flatten.toArray).runSyncUnsafe()

      //then
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      actualContent shouldBe a[Array[Byte]]
      actualContent shouldBe content.getBytes()
    }

    "downloadMultipart big object" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
      val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
      s3Resource.use(s3 => ob.consumeWith(s3.uploadMultipart(bucketName, key))).runSyncUnsafe()

      //when
      val actualContent: Array[Byte] =
        s3Resource.use(_.downloadMultipart(bucketName, key, 52428).toListL).map(_.flatten.toArray).runSyncUnsafe()

      //then
      val expectedArrayByte: Array[Byte] =
        ob.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).runSyncUnsafe()
      expectedArrayByte shouldBe actualContent
    }

    "downloading in multipart from a non existing bucket object returns failure" in {
      //given
      val bucket: String = "non-existing-bucket"
      val key: String = "non/existing/key"

      //when
      val f = s3Resource.use(_.downloadMultipart(bucket, key, 1).toListL).runToFuture
      sleep(100)

      //then
      f.value.get shouldBe a[Failure[NoSuchBucketException]]
      s3Resource.use(_.existsObject(bucket, key)).runSyncUnsafe() shouldBe false
    }

  }

}
