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

import java.io.FileInputStream
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.model.NoSuchKeyException

class MultipartDownloadObservableSuite
  extends AsyncFlatSpec with Matchers with MonixTaskTest with BeforeAndAfterAll with S3Fixture {

  private val bucketName = "multipart-download-test-bucket"
  override implicit val scheduler = Scheduler.io("multipart-download-observable-suite")

  override def beforeAll(): Unit = {
    super.beforeAll()
    s3Resource.use(_.createBucket(bucketName)).attempt.runSyncUnsafe() match {
      case Right(_) => info(s"Created s3 bucket $bucketName")
      case Left(e) => info(s"Failed to create s3 bucket $bucketName with exception: ${e.getMessage}")
    }
  }

  "MultipartDownloadObservable" should "downloadMultipart of small chunk size" in {
      val key: String = genKey.sample.get
      val content: String = Gen.identifier.sample.get

      for {
        _ <- unsafeS3.upload(bucketName, key, content.getBytes)
        actualContent <- unsafeS3.downloadMultipart(bucketName, key, 2).toListL.map(_.flatten.toArray)
        existsObject <- unsafeS3.existsObject(bucketName, key)
      } yield {
        existsObject shouldBe true
        actualContent shouldBe content.getBytes()
      }
    }

  it should "download 'multipart' in a single part" in {
    val key: String = genKey.sample.get
    val content: String = Gen.identifier.sample.get
    for {
      _ <- unsafeS3.upload(bucketName, key, content.getBytes)
      actualContent <- unsafeS3.downloadMultipart(bucketName, key, 52428).toListL.map(_.flatten.toArray)

      existsObject <- unsafeS3.existsObject(bucketName, key)
    } yield {
      existsObject shouldBe true
      actualContent shouldBe content.getBytes()
    }
  }

  it should "download a big object in multiparts" in {
      val key: String = genKey.sample.get
      val inputStream = Task(new FileInputStream(resourceFile("244KB.csv")))
      val fileData = Observable.fromInputStream(inputStream)
      for {
        expectedArrayByte <- fileData.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes)
        _ <- fileData.consumeWith(unsafeS3.uploadMultipart(bucketName, key))
        downloadedChunks <- unsafeS3.downloadMultipart(bucketName, key, 25000).toListL
      } yield {
        downloadedChunks.size shouldBe 10
        expectedArrayByte shouldBe downloadedChunks.flatten.toArray
      }
  }

  it should "fail when donwloading from a non existing object" in {
      val bucket: String = "non-existing-bucket"
      val key: String = "non/existing/key"

       unsafeS3.downloadMultipart(bucket, key).toListL.assertThrows[NoSuchKeyException] >>
         unsafeS3.existsObject(bucket, key).asserting(_ shouldBe false)
  }

  it should "fail if the `chunkSize` is negative" in {
    val negativeNum = Gen.chooseNum(-10, -1).sample.get
    unsafeS3.download("no-bucket", "no-key", Some(negativeNum)).assertThrows[IllegalArgumentException]
  }

  it should "fail if the `chunkSize` is zero" in {
    val chunkSize = 0
    unsafeS3.download("no-bucket", "no-key", Some(chunkSize)).assertThrows[IllegalArgumentException]
  }

}
