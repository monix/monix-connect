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
import software.amazon.awssdk.services.s3.model._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class ListObjectsObservableSuite
  extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"

  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(S3.createBucket(bucketName).runSyncUnsafe()) match {
      case Success(_) => info(s"Created S3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create S3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }

  s"${ListObjectsObservable}" should {

    "limit the list to the maximum number of objects" in {
      //given
      val n = 10
      val prefix = s"test-list-all-truncated/${nonEmptyString.value()}/"
      val keys: List[String] =
        Gen.listOfN(n, Gen.nonEmptyListOf(Gen.alphaChar).map(l => prefix + l.mkString)).sample.get
      val contents: List[String] = Gen.listOfN(n, Gen.alphaUpperStr).sample.get
      Task
        .sequence(keys.zip(contents).map { case (key, content) => S3.upload(bucketName, key, content.getBytes()) })
        .runSyncUnsafe()

      //when
      val s3Objects = S3.listObjects(bucketName, maxTotalKeys = Some(1), prefix = Some(prefix)).toListL.runSyncUnsafe()

      //then
      s3Objects.size shouldBe 1
      keys.contains(s3Objects.head.key) shouldBe true

      //and it should be truncated
      val request = S3RequestBuilder.listObjectsV2(bucketName, maxKeys = Some(1))
      Task.from(s3AsyncClient.listObjectsV2(request)).runSyncUnsafe().isTruncated shouldBe true
    }

    "list objects return continuationToken when set" in {
      //given
      val n = 120
      val prefix = s"test-list-continuation/${nonEmptyString.value()}/"
      val keys: List[String] =
        Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + nonEmptyString.value() + str)).sample.get
      val contents: List[String] = List.fill(n)(nonEmptyString.value())
      Task
        .sequence(keys.zip(contents).map { case (key, content) => S3.upload(bucketName, key, content.getBytes()) })
        .runSyncUnsafe()

      //when
      val response = Task.from(s3AsyncClient.listObjectsV2(S3RequestBuilder.listObjectsV2(bucketName, prefix = Some(prefix), maxKeys = Some(10)))).runSyncUnsafe()

      //then
      response.nextContinuationToken should not be null
      response.continuationToken shouldBe null
      response.isTruncated shouldBe true
    }

    "list all objects using the continuation token" in {
      //given
      val n = 2020
      val prefix = s"test-list-all-truncated/${nonEmptyString.value()}/"
      val keys: List[String] =
        Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + nonEmptyString.value() + str)).sample.get
      val contents: List[String] = List.fill(n)(nonEmptyString.value())
      Task
        .sequence(keys.zip(contents).map { case (key, content) => S3.upload(bucketName, key, content.getBytes()) })
        .runSyncUnsafe()

      //when
      val count = S3.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(n)).countL.runSyncUnsafe()

      //then
      count shouldBe n
    }

    "list a limited number of objects using the continuation token" in {
      //given
      val n = 1600
      val limit = 1300
      val prefix = s"test-list-limit-truncated/${nonEmptyString.value()}/"
      val keys: List[String] =
        Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + nonEmptyString.value() + str)).sample.get
      val contents: List[String] = List.fill(n)(nonEmptyString.value())
      Task
        .sequence(keys.zip(contents).map { case (key, content) => S3.upload(bucketName, key, content.getBytes()) })
        .runSyncUnsafe()

      //when
      val s3Objects = S3.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(limit)).toListL.runSyncUnsafe()

      //then
      s3Objects.size shouldBe limit
    }

    "list objects requisite of positive max total keys" in {
      //given/when
      val tryNegativeListObjects = Try(S3.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(-1)))
      val tryZeroListObjects = Try(S3.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(0)))
      val tryPositiveListObjects = Try(S3.listObjects(bucketName, prefix = Some("prefix"), maxTotalKeys = Some(1)))

      //then
      tryNegativeListObjects.isSuccess shouldBe false
      tryZeroListObjects.isSuccess shouldBe false
      tryPositiveListObjects.isSuccess shouldBe true
    }

    "list objects fails when bucket does not exists" in {
      //given/when
      val f = {
        for {
          a <- S3.listObjects("no-existing-bucket", prefix = Some("prefix")).toListL
          b <- Task.unit
        } yield b
      }.runToFuture
      sleep(400)

      //then
      f.value.get shouldBe a[Failure[NoSuchBucketException]]
    }

  }

}
