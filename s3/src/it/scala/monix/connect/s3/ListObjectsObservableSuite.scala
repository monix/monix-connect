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

import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.model._

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class ListObjectsObservableSuite
  extends AsyncFlatSpec with Matchers with MonixTaskSpec with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "list-observable-test-bucket"

  override implicit val scheduler = Scheduler.io("list-objects-observable-suite")
  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    s3Resource.use(_.createBucket(bucketName)).attempt.runSyncUnsafe() match {
      case Right(_) => info(s"Created S3 bucket $bucketName ")
      case Left(e) => info(s"Failed to create S3 bucket $bucketName with exception: ${e.getMessage}")
    }
  }

  s"$ListObjectsObservable" can "use a small limit of maximum number of objects listed" in {
    val n = 10
    val prefix = s"test-list-all-truncated/${genKey.sample.get}/"
    val keys: List[String] = Gen.listOfN(n, genKey.map(str => prefix + str)).sample.get
    val contents: List[String] = Gen.listOfN(n, Gen.alphaUpperStr).sample.get

      for {
        _ <- Task.sequence(keys.zip(contents).map {
          case (key, content) => unsafeS3.upload(bucketName, key, content.getBytes())
        })
        s3Objects <- unsafeS3.listObjects(bucketName, maxTotalKeys = Some(1), prefix = Some(prefix)).toListL
        listRequest <- {
          val request = S3RequestBuilder.listObjectsV2(bucketName, maxKeys = Some(1))
          Task.from(s3AsyncClient.listObjectsV2(request))
        }
      } yield {
        s3Objects.size shouldBe 1
        keys.contains(s3Objects.head.key) shouldBe true
        listRequest.isTruncated shouldBe true
      }
  }

  it should "return nextContinuationToken when set" in {
    val n = 120
    val prefix = s"test-list-continuation/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents = List.fill(n)(genKey.sample.get)
      Task
        .sequence(keys.zip(contents).map { case (key, content) => unsafeS3.upload(bucketName, key, content.getBytes()) })

    Task.sequence(keys.zip(contents).map { case (key, content) => unsafeS3.upload(bucketName, key, content.getBytes()) }) >>
    Task
     .from(
       s3AsyncClient.listObjectsV2(
         S3RequestBuilder.listObjectsV2(bucketName, prefix = Some(prefix), maxKeys = Some(10)))
     ).asserting{ response =>
      response.nextContinuationToken should not be null
      response.continuationToken shouldBe null
      response.isTruncated shouldBe true
    }
  }

  it should "list all objects" in {
    val n = 2020
    val prefix = s"test-list-all-truncated/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents: List[String] = List.fill(n)(genKey.sample.get)

   Task.traverse(keys.zip(contents)) {
     case (key, content) => unsafeS3.upload(bucketName, key, content.getBytes())
   } >> unsafeS3.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(n)).countL
    .asserting(_ shouldBe n)
  }

  it can "set a big limit of the number of objects returned" in {
    val n = 1600
    val limit = 1300
    val prefix = s"test-list-limit-truncated/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents: List[String] = List.fill(n)(genKey.sample.get)

    Task.traverse(keys.zip(contents)){
      case (key, content) => unsafeS3.upload(bucketName, key, content.getBytes())
    } >>
     unsafeS3.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(limit)).toListL
       .asserting(_.size shouldBe limit)
  }

  it must "require a positive max total keys" in {
     for {
       negativeListObjects <- unsafeS3.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(-1)).toListL.attempt
       zeroListObjects <- unsafeS3.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(0)).toListL.attempt
       positiveListObjects <- unsafeS3.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(1)).toListL.attempt
     } yield {
       negativeListObjects.isRight shouldBe false
       zeroListObjects.isRight shouldBe false
       positiveListObjects.isRight shouldBe true
     }
  }

  it should "fail when bucket does not exists" in {
    unsafeS3.listObjects("no-existing-bucket", prefix = Some("prefix"))
      .toListL
      .assertThrows[NoSuchBucketException]
  }

}
