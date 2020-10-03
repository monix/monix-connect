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

class MultipartDownloadObservableSuite
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

  s"${MultipartDownloadObservable}" should {

    "download a s3 object as byte array" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = Gen.alphaUpperStr.sample.get
      S3.upload(bucketName, key, content.getBytes).runSyncUnsafe()

      //when
      val t: Task[Array[Byte]] = S3.download(bucketName, key)

      //then
      whenReady(t.runToFuture) { actualContent: Array[Byte] =>
        S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
        actualContent shouldBe a[Array[Byte]]
        actualContent shouldBe content.getBytes()
      }
    }

    "download a s3 object bigger than 1MB as byte array" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
      val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
      val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
        S3.uploadMultipart(bucketName, key)
      val _: CompleteMultipartUploadResponse = ob.consumeWith(consumer).runSyncUnsafe()

      //when
      val t = S3.download(bucketName, key)

      //then
      whenReady(t.runToFuture) { actualContent: Array[Byte] =>
        val expectedArrayByte = ob.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).runSyncUnsafe()
        S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
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
      S3.upload(bucketName, key, content.getBytes).runSyncUnsafe()

      //when
      val t: Task[Array[Byte]] = S3.download(bucketName, key, Some(n))

      //then
      whenReady(t.runToFuture) { partialContent: Array[Byte] =>
        S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
        partialContent shouldBe a[Array[Byte]]
        partialContent shouldBe content.getBytes().take(n)
      }
    }

    "download fails if the numberOfBytes is negative" in {
      //given
      val negativeNum = Gen.chooseNum(-10, 0).sample.get
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString

      //when
      val t: Try[Task[Array[Byte]]] = Try(S3.download(bucketName, key, Some(negativeNum)))

      //then
      t.isFailure shouldBe true
    }

    "download from a non existing key returns failed task" in {
      //given
      val key: String = "non-existing-key"

      //when
      val f: Future[Array[Byte]] = S3.download(bucketName, key).runToFuture(global)
      sleep(400)

      //then
      f.value.get shouldBe a[Failure[NoSuchKeyException]]
    }

    "downloadMultipart of small chunk size" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = nonEmptyString.value()
      S3.upload(bucketName, key, content.getBytes).runSyncUnsafe()

      //when
      val actualContent: Array[Byte] =
        S3.downloadMultipart(bucketName, key, 2).toListL.map(_.flatten.toArray).runSyncUnsafe()

      //then
      S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
      actualContent shouldBe a[Array[Byte]]
      actualContent shouldBe content.getBytes()
    }

    "downloadMultipart in one part" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = nonEmptyString.value()
      S3.upload(bucketName, key, content.getBytes).runSyncUnsafe()

      //when
      val actualContent: Array[Byte] =
        S3.downloadMultipart(bucketName, key, 52428).toListL.map(_.flatten.toArray).runSyncUnsafe()

      //then
      S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
      actualContent shouldBe a[Array[Byte]]
      actualContent shouldBe content.getBytes()
    }

    "downloadMultipart big object" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
      val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
      val _ = ob.consumeWith(S3.uploadMultipart(bucketName, key)).runSyncUnsafe()

      //when
      val actualContent: Array[Byte] =
        S3.downloadMultipart(bucketName, key, 52428).toListL.map(_.flatten.toArray).runSyncUnsafe()

      //then
      val expectedArrayByte: Array[Byte] =
        ob.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).runSyncUnsafe()
      S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
      actualContent shouldBe a[Array[Byte]]
      actualContent shouldBe expectedArrayByte
    }

    "downloadMultipart from a non existing object returns an empty byte array" in {
      //given
      val key: String = "non/existing/key"
      //when
      val f = S3.downloadMultipart(bucketName, key, 1).toListL.runToFuture
      sleep(100)

      //then
      f.value.get shouldBe a[Failure[NoSuchKeyException]]
      S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe false
    }

    "downloading in multipart from a non existing bucket object returns failure" in {
      //given
      val bucket: String = "non-existing-bucket"
      val key: String = "non/existing/key"

      //when
      val f = S3.downloadMultipart(bucket, key, 1).toListL.runToFuture
      sleep(100)

      //then
      f.value.get shouldBe a[Failure[NoSuchBucketException]]
      S3.existsObject(bucket, key).runSyncUnsafe() shouldBe false
    }

  }

}
