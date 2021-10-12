package monix.connect.s3

import java.io.FileInputStream
import monix.eval.Task
import monix.execution.Scheduler
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadResponse, CopyObjectResponse, DeleteBucketResponse, DeleteObjectResponse, NoSuchBucketException, NoSuchKeyException, PutObjectResponse}
import org.scalatest.wordspec.{AnyWordSpecLike, AsyncWordSpec}
import org.scalatest.matchers.should.Matchers

import Thread.sleep
import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import monix.testing.scalatest.MonixTaskSpec
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

@deprecated("0.5.0")
class S3ITest
  extends AsyncWordSpec with MonixTaskSpec with Matchers with BeforeAndAfterAll with S3Fixture {

  private val bucketName = "sample-bucket"
  override implicit val scheduler = Scheduler.io("s3-it-test")

  override def beforeAll(): Unit = {
    super.beforeAll()
    S3.createBucket(bucketName).attempt.runSyncUnsafe() match {
      case Right(_) => info(s"Created S3 bucket $bucketName ")
      case Left(e) => info(s"Failed to create S3 bucket $bucketName with exception: ${e.getMessage}")
    }
  }

  s"$S3" should {

    "implement putObject method" that {

      s"uploads the passed chunk of bytes to the respective s3 bucket and key" when {

        "contentLength and contentType are not defined and therefore infered by the method" in {
          //given
          val key: String = genKey.sample.get
          val content: String = Gen.alphaUpperStr.sample.get

          //when
          for {
            putResponse <- S3.upload(bucketName, key, content.getBytes()).asyncBoundary
            s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
          } yield {
            putResponse shouldBe a[PutObjectResponse]
            s3Object shouldBe content.getBytes()
          }
        }

        "contentLength and contentType are defined respectively as the array length and 'application/json'" in {
          val key: String = genKey.sample.get
          val content: String = Gen.alphaUpperStr.sample.get

          for {
            putResponse <- S3.upload(bucketName, key, content.getBytes())
            s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
          } yield {
            putResponse shouldBe a[PutObjectResponse]
            s3Object shouldBe content.getBytes()
          }
        }
        }

        "the chunk is empty" in {
          //given
          val key: String = genKey.sample.get
          val content: Array[Byte] = Array.emptyByteArray

          //when
          for {
            putResponse <- S3.upload(bucketName, key, content)
            s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
          } yield {
            putResponse shouldBe a[PutObjectResponse]
            s3Object shouldBe content
          }
      }
    }
  }

  it should {

    "download a s3 object as byte array" in {
      val key: String = genKey.sample.get
      val content: String = Gen.alphaUpperStr.sample.get

      for {
        _ <- S3.upload(bucketName, key, content.getBytes)
        existsObject <- S3.existsObject(bucketName, key)
        actualContent <- S3.download(bucketName, key)
      } yield {
        existsObject shouldBe true
        actualContent shouldBe content.getBytes()
      }
    }

    "download a s3 object bigger than 1MB as byte array" in {
      val key: String = genKey.sample.get
      val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
      val testData: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
      val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
        S3.uploadMultipart(bucketName, key)
      for {
        completeMultipartUpload <- testData.consumeWith(consumer)
        actualContent <- S3.download(bucketName, key)
        expectedArrayByte <- testData.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes)
        existsObject <- S3.existsObject(bucketName, key)

      } yield {
        completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
        existsObject shouldBe true
        actualContent.size shouldBe expectedArrayByte.size
        actualContent shouldBe expectedArrayByte
      }
    }

    "download the first n bytes form an object" in {
      //given
      val n = 5
      val key: String = genKey.sample.get
      val content: String = Gen.identifier.sample.get
      for {
        _ <- S3.upload(bucketName, key, content.getBytes)
        partialContent <- S3.download(bucketName, key, Some(n))
        existsObject <- S3.existsObject(bucketName, key)
      } yield {
        existsObject shouldBe true
        partialContent shouldBe content.getBytes().take(n)
      }
    }

    "download fails if the numberOfBytes is negative" in {
      //given
      val negativeNum = Gen.chooseNum(-10, 0).sample.get
      val key: String = genKey.sample.get

      //when
      S3.download(bucketName, key, Some(negativeNum)).assertThrows[IllegalArgumentException]
    }

    "download from a non existing key returns failed task" in {
      val key: String = "non-existing-key"
      S3.download(bucketName, key).assertThrows[NoSuchKeyException]
    }

    "downloadMultipart of small chunk size" in {
      val key: String = genKey.sample.get
      val content: String = Gen.identifier.sample.get

      for {
        _ <- S3.upload(bucketName, key, content.getBytes)
        existsObject <- S3.existsObject(bucketName, key)
        actualContent <- S3.downloadMultipart(bucketName, key, 2).toListL.map(_.flatten.toArray)
      } yield {
        existsObject shouldBe true
        actualContent shouldBe content.getBytes()
      }
    }

  }

  it should {

    "copy an object to a different location within the same bucket" in {
      //given
      val sourceKey = genKey.sample.get
      val content = Gen.identifier.sample.get.getBytes()
      S3.upload(bucketName, sourceKey, content).runSyncUnsafe()

      //and
      val destinationKey = Gen.identifier.sample.get

      //when
      val copyObjectResponse = S3.copyObject(bucketName, sourceKey, bucketName, destinationKey).runSyncUnsafe()

      //then
      copyObjectResponse shouldBe a[CopyObjectResponse]
      S3.download(bucketName, destinationKey).runSyncUnsafe() shouldBe content
    }

    "copy an object to a different location in a different bucket" in {
      //given
      val sourceKey = genKey.sample.get
      val content = Gen.identifier.sample.get.getBytes()
      S3.upload(bucketName, sourceKey, content).runSyncUnsafe()

      //and
      val destinationBucket = genBucketName.sample.get
      val destinationKey = genKey.sample.get
      S3.createBucket(destinationBucket).runSyncUnsafe()

      //when
      val copyObjectResponse = S3.copyObject(bucketName, sourceKey, destinationBucket, destinationKey).runSyncUnsafe()

      //then
      copyObjectResponse shouldBe a[CopyObjectResponse]
      S3.download(destinationBucket, destinationKey).runSyncUnsafe() shouldBe content
    }

  }

  it should {

    "correctly perform a multipart upload" when {

      "a single chunk is passed to the consumer" in {
        //given
        val key = genKey.sample.get
        val content: Array[Byte] = Gen.identifier.sample.get.getBytes

        for {
          completeMultipartUpload <- Observable.pure(content).consumeWith(S3.uploadMultipart(bucketName, key))
          existsObject <- S3.existsObject(bucketName, key)
          s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
        } yield {
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          existsObject shouldBe true
          s3Object shouldBe content
        }
      }

      "multiple chunks (of less than minimum size) are passed" in {
        //given
        val key: String = genKey.sample.get
        val chunks: List[Array[Byte]] = Gen.listOfN(10, Gen.identifier.map(_.getBytes)).sample.get

        for {
          completeMultipartUpload <- Observable.fromIterable(chunks).consumeWith(S3.uploadMultipart(bucketName, key))
          existsObject <- S3.existsObject(bucketName, key)
          s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
        } yield {
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          existsObject shouldBe true
          s3Object shouldBe chunks.flatten
        }
      }

      "a single chunk of size (1MB)" in {
        //given
        val key = genKey.sample.get
        val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
        val testData: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)

        for{
          completeMultipartUpload <- testData.consumeWith( S3.uploadMultipart(bucketName, key))
          existsObject <- S3.existsObject(bucketName, key)
          expectedArrayByte <- testData.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL
          s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
        } yield {
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          existsObject shouldBe true
          s3Object shouldBe expectedArrayByte
        }
      }

      "multiple chunks bigger than minimum size (5MB)" in {
        //given
        val key = genKey.sample.get
        val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
        val testData: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
          .foldLeft(Array.emptyByteArray)((acc, chunk) => acc ++ chunk ++ chunk ++ chunk ++ chunk ++ chunk) //duplicates each chunk * 5

        for {
          completeMultipartUpload <- testData.consumeWith(S3.uploadMultipart(bucketName, key))
          expectedArrayByte <- testData.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL
          existsObject <- S3.existsObject(bucketName, key)
          s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
        } yield {
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          existsObject shouldBe true
          s3Object shouldBe expectedArrayByte
        }
      }
    }
  }

  it should {

    "create and delete a bucket" in {
      //given
      val bucket = genBucketName.sample.get
      for {
        _ <- S3.createBucket(bucket)
        existedBefore <- S3.existsBucket(bucket)
        deleteResponse <- S3.deleteBucket(bucket)
        existsAfterDeletion <- S3.existsBucket(bucket)
      } yield {
        deleteResponse shouldBe a[DeleteBucketResponse]
        existedBefore shouldBe true
        existsAfterDeletion shouldBe false
      }
    }

    "delete returns NoSuchBucketException when the bucket did not exist" in {
      //given
      val bucket = Gen.identifier.sample.get
      for {
        existedBefore <- S3.existsBucket(bucket)
        deleteResponse <- S3.deleteBucket(bucket).attempt
        existsAfterDeletion <- S3.existsBucket(bucket)
      } yield {
        deleteResponse.isLeft shouldBe true
        deleteResponse.left.get shouldBe a[NoSuchBucketException]
        existedBefore shouldBe false
        existsAfterDeletion shouldBe false
      }
    }

    "delete a object" in {
      //given
      val key = Gen.identifier.sample.get
      val content = Gen.identifier.sample.get.getBytes()
      for {
        _ <- S3.upload(bucketName, key, content)
        existedBefore <- S3.existsObject(bucketName, key)
        deleteResponse <- S3.deleteObject(bucketName, key)
        existsAfterDeletion <- S3.existsObject(bucketName, key)
      } yield {
        existedBefore shouldBe true
        deleteResponse shouldBe a[DeleteObjectResponse]
        existsAfterDeletion shouldBe false
      }
    }

    "check if a bucket exists" in {
      val bucketNameA = genBucketName.sample.get
      val bucketNameB = genBucketName.sample.get

      for {
        _ <- S3.createBucket(bucketNameA)
        existsA <- S3.existsBucket(bucketNameA)
        existsB <- S3.existsBucket(bucketNameB)
      } yield {
        existsA shouldBe true
        existsB shouldBe false
      }
    }

    "list existing buckets" in {
      val bucketName1 = genBucketName.sample.get
      val bucketName2 = genBucketName.sample.get

      for {
        initialBuckets <- S3.listBuckets().toListL
        _ <- S3.createBucket(bucketName1)
        _ <- S3.createBucket(bucketName2)
        buckets <- S3.listBuckets().toListL
      } yield {
        buckets.size - initialBuckets.size shouldBe 2
        (buckets diff initialBuckets).map(_.name) should contain theSameElementsAs List(bucketName1, bucketName2)
      }
    }

    "check if an object exists" in {
      val prefix = s"test-exists-object/${Gen.identifier.sample.get}/"
      val key: String = prefix + Gen.identifier.sample.get

      for {
        _ <- S3.upload(bucketName, key, "dummy content".getBytes())
        exists1 <- S3.existsObject(bucketName, key = key)
        exists2 <- S3.existsObject(bucketName, key = "non existing key")
      } yield {
        exists1 shouldBe true
        exists2 shouldBe false
      }
    }

    "list all objects" in {
        val n = 1000
        val prefix = s"test-list-all-truncated/${genKey.sample.get}/"
        val keys: List[String] = Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
        val contents: List[String] = List.fill(n)(genKey.sample.get)
        Task.traverse(keys.zip(contents)) { case (key, content) =>
          S3.upload(bucketName, key, content.getBytes())
        } >>
          S3.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(n)).countL
            .asserting(_ shouldBe n)
    }
  }
}
