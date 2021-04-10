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

import java.lang.Thread.sleep

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.s3.model._

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class ListObjectsObservableSuite
  extends AnyFlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "list-observable-test"

  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(s3Resource.use(_.createBucket(bucketName)).runSyncUnsafe()) match {
      case Success(_) => info(s"Created S3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create S3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }

  s"${ListObjectsObservable}" can "use a small limit of maximum number of objects listed" in {
    //given
    val n = 10
    val prefix = s"test-list-all-truncated/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, genKey.map(str => prefix + str)).sample.get
    val contents: List[String] = Gen.listOfN(n, Gen.alphaUpperStr).sample.get

    s3Resource.use { s3 =>
      (for {
        _ <- Task.sequence(keys.zip(contents).map {
          case (key, content) => s3.upload(bucketName, key, content.getBytes())
        })
        s3Objects <- s3.listObjects(bucketName, maxTotalKeys = Some(1), prefix = Some(prefix)).toListL
        listRequest <- {
          val request = S3RequestBuilder.listObjectsV2(bucketName, maxKeys = Some(1))
          Task.from(s3AsyncClient.listObjectsV2(request))
        }
      } yield {
        s3Objects.size shouldBe 1
        keys.contains(s3Objects.head.key) shouldBe true
        listRequest.isTruncated shouldBe true
      })
    }.runSyncUnsafe()
  }

  it should "return nextContinuationToken when set" in {
    //given
    val n = 120
    val prefix = s"test-list-continuation/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents: List[String] = List.fill(n)(genKey.sample.get)
    s3Resource.use { s3 =>
      Task
        .sequence(keys.zip(contents).map { case (key, content) => s3.upload(bucketName, key, content.getBytes()) })
    }.runSyncUnsafe()

    //when
    val response = Task
      .from(
        s3AsyncClient.listObjectsV2(
          S3RequestBuilder.listObjectsV2(bucketName, prefix = Some(prefix), maxKeys = Some(10))))
      .runSyncUnsafe()

    //then
    response.nextContinuationToken should not be null
    response.continuationToken shouldBe null
    response.isTruncated shouldBe true
  }

  it should "list all objects" in {
    //given
    val n = 2020
    val prefix = s"test-list-all-truncated/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents: List[String] = List.fill(n)(genKey.sample.get)

    (s3Resource.use { s3 =>
      for {
        _ <- Task.sequence(keys.zip(contents).map {
          case (key, content) => s3.upload(bucketName, key, content.getBytes())
        })
        count <- s3.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(n)).countL
      } yield (count shouldBe n)
    }).runSyncUnsafe()
  }

  it can "set a big limit of the number of objects returned" in {
    //given
    val n = 1600
    val limit = 1300
    val prefix = s"test-list-limit-truncated/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents: List[String] = List.fill(n)(genKey.sample.get)

    s3Resource.use { s3 =>
      Task
        .sequence(keys.zip(contents).map { case (key, content) => s3.upload(bucketName, key, content.getBytes()) })
    }.runSyncUnsafe()

    //when
    val s3Objects = s3Resource
      .use(_.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(limit)).toListL)
      .runSyncUnsafe()

    //then
    s3Objects.size shouldBe limit
  }

  it must "require a positive max total keys" in {
    //given/when
    val tryNegativeListObjects =
      s3Resource.use(_.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(-1)).toListL).runToFuture
    val tryZeroListObjects =
      s3Resource.use(_.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(0)).toListL).runToFuture
    val tryPositiveListObjects =
      s3Resource.use(_.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(1)).toListL).runToFuture

    sleep(1000)
    //then
    tryNegativeListObjects.value.get.isSuccess shouldBe false
    tryZeroListObjects.value.get.isSuccess shouldBe false
    tryPositiveListObjects.value.get.isSuccess shouldBe true
  }

  it should "fail when bucket does not exists" in {
    //given/when
    val f = s3Resource.use(_.listObjects("no-existing-bucket", prefix = Some("prefix")).toListL).runToFuture
    sleep(200)

    //then
    f.value.get.isFailure shouldBe true
    f.value.get.failed.get shouldBe a[NoSuchBucketException]
  }

}
