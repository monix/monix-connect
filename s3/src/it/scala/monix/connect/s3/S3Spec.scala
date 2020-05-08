package monix.connect.s3

import java.io.FileInputStream

import monix.eval.Task
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadResponse,
  PutObjectResponse
}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.util.{Failure, Success, Try}

class S3Spec
  extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"
  implicit val s3Client = s3AsyncClient

  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  s"${S3}" should {

    "implement putObject method" that {

      s"uploads the passed chunk of bytes to the respective s3 bucket and key" when {

        "contentLength and contentType are not defined and therefore infered by the method" in {
          //given
          val key = Gen.alphaLowerStr.sample.get
          val content = Gen.alphaUpperStr.sample.get

          //when
          val t: Task[PutObjectResponse] = S3.putObject(bucketName, key, content.getBytes())

          //then
          whenReady(t.runToFuture) { putResponse =>
            val s3Object: Array[Byte] = download(bucketName, key).get
            putResponse shouldBe a[PutObjectResponse]
            s3Object shouldBe content.getBytes()
          }
        }

        "contentLength and contentType are defined respectively as the array lenght and 'application/json'" in {
          //given
          val key = Gen.alphaLowerStr.sample.get
          val content: String = Gen.alphaUpperStr.sample.get

          //when
          val t: Task[PutObjectResponse] = S3.putObject(
            bucketName,
            key,
            content.getBytes(),
            Some(content.length)
          )

          //then
          whenReady(t.runToFuture) { putResponse =>
            val s3Object: Array[Byte] = download(bucketName, key).get
            putResponse shouldBe a[PutObjectResponse]
            s3Object shouldBe content.getBytes()
          }
        }

        "the payload is bigger" in {
          //given
          val key: String = Gen.alphaLowerStr.sample.get
          val content: Array[Byte] = downloadFromFile(resourceFile("empty.txt")).get

          //when
          val t: Task[PutObjectResponse] = S3.putObject(bucketName, key, content)

          //then
          whenReady(t.runToFuture) { putResponse =>
            eventually {
              val s3Object: Array[Byte] = download(bucketName, key).get
              putResponse shouldBe a[PutObjectResponse]
              s3Object shouldBe content
            }
          }
        }
        "the chunk is empty" in {
          //given
          val key: String = Gen.alphaLowerStr.sample.get
          val content: Array[Byte] = Array.emptyByteArray

          //when
          val t: Task[PutObjectResponse] = S3.putObject(bucketName, key, content)

          //then
          whenReady(t.runToFuture) { putResponse =>
            eventually {
              val s3Object: Array[Byte] = download(bucketName, key).get
              putResponse shouldBe a[PutObjectResponse]
              s3Object shouldBe content
            }
          }
        }
      }
    }
  }

  it should {

    "implement a getObject method" that {

      "downloads a s3 object as byte array" in {
        //given
        val key: String = Gen.alphaLowerStr.sample.get
        val content: String = Gen.alphaUpperStr.sample.get
        s3SyncClient.putObject(bucketName, key, content)

        //when
        val t: Task[Array[Byte]] = S3.getObject(bucketName, key)

        //then
        whenReady(t.runToFuture) { actualContent: Array[Byte] =>
          s3SyncClient.doesObjectExist(bucketName, key) shouldBe true
          actualContent shouldBe a[Array[Byte]]
          actualContent shouldBe content.getBytes()
        }
      }

      "download a s3 object bigger than 1MB as byte array" in {
        //given
        val key = Gen.alphaLowerStr.sample.get
        val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
        val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
        val consumer: Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] =
          S3.multipartUpload(bucketName, key)
        val _: CompleteMultipartUploadResponse = ob.consumeWith(consumer).flatten.runSyncUnsafe()

        //when
        val t = S3.getObject(bucketName, key)

        //then
        whenReady(t.runToFuture) { actualContent: Array[Byte] =>
          val expectedArrayByte = ob.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).runSyncUnsafe()
          s3SyncClient.doesObjectExist(bucketName, key) shouldBe true
          actualContent shouldBe a[Array[Byte]]
          actualContent.size shouldBe expectedArrayByte.size
          actualContent shouldBe expectedArrayByte
        }
      }
    }
  }

  it should {

    "correctly perform a multipart upload" when {

      "a single chunk is passed to the consumer" in {
        //given
        val key = Gen.alphaLowerStr.sample.get
        val content: Array[Byte] = Gen.alphaUpperStr.sample.get.getBytes
        val consumer: Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] =
          S3.multipartUpload(bucketName, key)
        val ob = Observable.pure(content)

        //when
        val t: Task[CompleteMultipartUploadResponse] = ob.consumeWith(consumer).flatten

        //then
        whenReady(t.runToFuture) { completeMultipartUpload =>
          val s3Object: Array[Byte] = download(bucketName, key).get
          s3SyncClient.doesObjectExist(bucketName, key) shouldBe true
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          s3Object shouldBe content
        }
      }

      "multiple chunks (of less than minimum size) are passed" in {
        //given
        val key = Gen.alphaLowerStr.sample.get
        val chunks: List[Array[Byte]] = Gen.listOfN(10, Gen.alphaUpperStr).map(_.map(_.getBytes)).sample.get
        val consumer: Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] =
          S3.multipartUpload(bucketName, key)
        val ob: Observable[Array[Byte]] = Observable.fromIterable(chunks)

        //when
        val t: Task[CompleteMultipartUploadResponse] = ob.consumeWith(consumer).flatten

        //then
        whenReady(t.runToFuture) { completeMultipartUpload =>
          eventually {
            val s3Object: Array[Byte] = download(bucketName, key).get
            s3SyncClient.doesObjectExist(bucketName, key) shouldBe true
            completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
            s3Object shouldBe chunks.flatten
          }
        }
      }

      "a single chunk of size (1MB)" in {
        //given
        val key = Gen.alphaLowerStr.sample.get
        val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
        val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
        val consumer: Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] =
          S3.multipartUpload(bucketName, key)

        //when
        val t: Task[CompleteMultipartUploadResponse] = ob.consumeWith(consumer).flatten

        //then
        val expectedArrayByte = ob.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL.runSyncUnsafe()
        whenReady(t.runToFuture) { completeMultipartUpload =>
          val s3Object: Array[Byte] = download(bucketName, key).get
          s3SyncClient.doesObjectExist(bucketName, key) shouldBe true
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          s3Object shouldBe expectedArrayByte
        }
      }

      "multiple chunks bigger than minimum size (5MB)" in {
        //given
        val key = Gen.alphaLowerStr.sample.get
        val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
        val ob: Observable[Array[Byte]] = Observable
          .fromInputStream(inputStream)
          .foldLeft(Array.emptyByteArray)((acc, chunk) => acc ++ chunk ++ chunk ++ chunk ++ chunk ++ chunk ++ chunk) //duplicates each chunk * 6
        val consumer: Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] =
          S3.multipartUpload(bucketName, key)

        //when
        val t: Task[CompleteMultipartUploadResponse] = ob.consumeWith(consumer).flatten

        //then
        val expectedArrayByte = ob.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL.runSyncUnsafe()
        whenReady(t.runToFuture) { completeMultipartUpload =>
          val s3Object: Array[Byte] = download(bucketName, key).get
          s3SyncClient.doesObjectExist(bucketName, key) shouldBe true
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          s3Object shouldBe expectedArrayByte
        }
      }
    }

  }

  def genPart(): Gen[String] = Gen.oneOf(Seq(List.fill(1000)(Gen.alphaStr.sample.get).mkString("_")))

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(s3SyncClient.createBucket(bucketName)) match {
      case Success(_) => info(s"Created S3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create s3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }
}
