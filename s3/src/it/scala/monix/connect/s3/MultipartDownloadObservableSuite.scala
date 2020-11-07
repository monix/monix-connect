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

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.NoSuchBucketException

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class MultipartDownloadObservableSuite
  extends AnyFlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "download-test"

  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(s3Resource.use(_.createBucket(bucketName)).runSyncUnsafe()) match {
      case Success(_) => info(s"Created s3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create s3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }

  "MultipartDownloadObservable" should "downloadMultipart of small chunk size" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = nonEmptyString.value()
      s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()

      //when
      val actualContent: Array[Byte] =
        s3Resource.use(_.downloadMultipart(bucketName, key, 2).toListL.map(_.flatten.toArray)).runSyncUnsafe()

      //then
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      actualContent shouldBe a[Array[Byte]]
      actualContent shouldBe content.getBytes()
    }

  it should "download 'multipart' in a single part" in {
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

  it should "download a big object in multiparts" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val inputStream = Task(new FileInputStream(resourceFile("244KB.csv")))
      val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
      s3Resource.use(s3 => ob.consumeWith(s3.uploadMultipart(bucketName, key))).runSyncUnsafe()

      //when
      val downloadedChunks: List[Array[Byte]] =
        s3Resource.use(_.downloadMultipart(bucketName, key, 25000).toListL).runSyncUnsafe()

      //then
      val expectedArrayByte: Array[Byte] =
        ob.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).runSyncUnsafe()
      downloadedChunks.size shouldBe 10
      expectedArrayByte shouldBe downloadedChunks.flatten
    }

  it should "fail when donwloading from a non existing object" in {
      //given
      val bucket: String = "non-existing-bucket"
      val key: String = "non/existing/key"

      //when
      val f = s3Resource.use(_.downloadMultipart(bucket, key).toListL).runToFuture

      //then
      Await.ready(f, 1.seconds)
      f.value.get shouldBe a[Failure[NoSuchBucketException]]
      s3Resource.use(_.existsObject(bucket, key)).runSyncUnsafe() shouldBe false
    }

  it should "fail if the `chunkSize` is negative" in {
      //given
      val negativeNum = Gen.chooseNum(-10, -1).sample.get

      //when
      val f = s3Resource.use(_.download("no-bucket", "no-key", Some(negativeNum)))
        .runToFuture

      //then
      Await.ready(f, 3.seconds)
      f.value.get.isFailure shouldBe true
  }

  it should "fail if the `chunkSize` is zero" in {
    //given
    val chunkSize = 0

    //when
    val f = s3Resource.use(_.download("no-bucket", "no-key", Some(chunkSize)))
      .runToFuture

    //then
    Await.ready(f, 3.seconds)
    f.value.get.isFailure shouldBe true
  }

}
