package monix.connect.s3

import java.io.FileInputStream

import monix.eval.Task
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadResponse, CopyObjectResponse, NoSuchBucketException, NoSuchKeyException, PutObjectResponse}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import Thread.sleep

import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import software.amazon.awssdk.regions.Region

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class S3Suite
  extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"
  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)
  private val s3Resource = S3.createWith(staticCredProvider, Region.AWS_GLOBAL, Some(minioEndPoint), Some(httpClient))

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(s3Resource.use(_.createBucket(bucketName)).runSyncUnsafe()) match {
      case Success(_) => info(s"Created s3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create s3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }

  s"${S3}" should {

    "implement putObject method" that {

      s"uploads the passed chunk of bytes to the respective s3 bucket and key" when {

        "contentLength and contentType are not defined and therefore infered by the method" in {
          //given
          val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
          val content: String = Gen.alphaUpperStr.sample.get

          //when
          val t: Task[PutObjectResponse] = s3Resource.use(_.upload(bucketName, key, content.getBytes())).asyncBoundary

          //then
          whenReady(t.runToFuture) { putResponse =>
            val s3Object: Array[Byte] = download(bucketName, key).get
            putResponse shouldBe a[PutObjectResponse]
            s3Object shouldBe content.getBytes()
          }
        }

        "contentLength and contentType are defined respectively as the array length and 'application/json'" in {
          //given
          val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
          val content: String = Gen.alphaUpperStr.sample.get

          //when
          val t: Task[PutObjectResponse] = s3Resource.use(_.upload(bucketName, key, content.getBytes()))

          //then
          whenReady(t.runToFuture) { putResponse =>
            val s3Object: Array[Byte] = download(bucketName, key).get
            putResponse shouldBe a[PutObjectResponse]
            s3Object shouldBe content.getBytes()
          }
        }

        "the payload is bigger" in {
          //given
          val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
          val content: Array[Byte] = downloadFromFile(resourceFile("empty.txt")).get

          //when
          val t: Task[PutObjectResponse] = s3Resource.use(_.upload(bucketName, key, content))

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
          val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
          val content: Array[Byte] = Array.emptyByteArray

          //when
          val t: Task[PutObjectResponse] = s3Resource.use(_.upload(bucketName, key, content))

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
      val negativeNum = Gen.chooseNum(-10, 0).sample.get
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString

      //when
      val f: Future[Array[Byte]] = s3Resource.use(_.download(bucketName, key, Some(negativeNum))).runToFuture

      //then

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

    "downloadMultipart with small chunk size" in {
      //given
      val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val content: String = nonEmptyString.value()
      s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()

      //when
      val actualContent: Array[Byte] =
        s3Resource.use(_.downloadMultipart(bucketName, key, 2).toListL).map(_.flatten.toArray).runSyncUnsafe()

      //then
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      actualContent shouldBe a[Array[Byte]]
      actualContent shouldBe content.getBytes()
    }

  }

  it should {

    "copy an object to a different location within the same bucket" in {
      //given
      val sourceKey = nonEmptyString.value()
      val content = nonEmptyString.value().getBytes()
      s3Resource.use(_.upload(bucketName, sourceKey, content)).runSyncUnsafe()

      //and
      val destinationKey = nonEmptyString.value()

      //when
      val copyObjectResponse = s3Resource.use(_.copyObject(bucketName, sourceKey, bucketName, destinationKey)).runSyncUnsafe()

      //then
      copyObjectResponse shouldBe a[CopyObjectResponse]
      s3Resource.use(_.download(bucketName, destinationKey)).runSyncUnsafe() shouldBe content
    }

    "copy an object to a different location in a different bucket" in {
      //given
      val sourceKey = nonEmptyString.value()
      val content = nonEmptyString.value().getBytes()
      s3Resource.use(_.upload(bucketName, sourceKey, content)).runSyncUnsafe()

      //and
      val destinationBucket = nonEmptyString.value()
      val destinationKey = nonEmptyString.value()
      s3Resource.use(_.createBucket(destinationBucket)).runSyncUnsafe()

      //when
      val copyObjectResponse = s3Resource.use(_.copyObject(bucketName, sourceKey, destinationBucket, destinationKey)).runSyncUnsafe()

      //then
      copyObjectResponse shouldBe a[CopyObjectResponse]
      s3Resource.use(_.download(destinationBucket, destinationKey)).runSyncUnsafe() shouldBe content
    }

  }

  it should {

    "correctly perform a multipart upload" when {

      "a single chunk is passed to the consumer" in {
        //given
        val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
        val content: Array[Byte] = Gen.alphaUpperStr.sample.get.getBytes


        //when
        val t = s3Resource.use { s3 =>
          Observable.pure(content).consumeWith(s3.uploadMultipart(bucketName, key))
        }

        //then
        whenReady(t.runToFuture) { completeMultipartUpload =>
          val s3Object: Array[Byte] = download(bucketName, key).get
          s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          s3Object shouldBe content
        }
      }

      "multiple chunks (of less than minimum size) are passed" in {
        //given
        val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
        val chunks: List[Array[Byte]] = Gen.listOfN(10, Gen.alphaUpperStr).map(_.map(_.getBytes)).sample.get

        //when
        val t = s3Resource.use { s3 =>
          Observable.fromIterable(chunks).consumeWith(s3.uploadMultipart(bucketName, key))
        }

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

      "a single chunk of size (1MB)" in {
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

      "multiple chunks bigger than minimum size (5MB)" in {
        //given
        val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
        val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
        val ob: Observable[Array[Byte]] = Observable
          .fromInputStream(inputStream)
          .foldLeft(Array.emptyByteArray)((acc, chunk) => acc ++ chunk ++ chunk ++ chunk ++ chunk ++ chunk) //duplicates each chunk * 5
        val response = s3Resource.use {s3 =>
        val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
          s3.uploadMultipart(bucketName, key)
        ob.consumeWith(consumer)
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
    }
  }

  it should {

    "create and delete a bucket" in {
      //given
      val bucket = nonEmptyString.value()
      s3Resource.use(_.createBucket(bucket)).runSyncUnsafe()
      val existedBefore = s3Resource.use(_.existsBucket(bucket)).runSyncUnsafe()

      //when
      s3Resource.use(_.deleteBucket(bucket)).runSyncUnsafe()

      //then
      val existsAfterDeletion = s3Resource.use(_.existsBucket(bucket)).runSyncUnsafe()
      existedBefore shouldBe true
      existsAfterDeletion shouldBe false
    }

    "delete returns NoSuchBucketException when the bucket did not exist" in {
      //given
      val bucket = nonEmptyString.value()
      val existedBefore = s3Resource.use(_.existsBucket(bucket)).runSyncUnsafe()

      //when
      val f = s3Resource.use(_.deleteBucket(bucket)).runToFuture(global)
      sleep(400)

      //then
      f.value.get shouldBe a[Failure[NoSuchBucketException]]
      val existsAfterDeletion = s3Resource.use(_.existsBucket(bucket)).runSyncUnsafe()
      existedBefore shouldBe false
      existsAfterDeletion shouldBe false
    }

    "delete a object" in {
      //given
      val key = nonEmptyString.value()
      val content = nonEmptyString.value().getBytes()
      s3Resource.use(_.upload(bucketName, key, content)).runSyncUnsafe()
      val existedBefore = s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe()

      //when
      s3Resource.use(_.deleteObject(bucketName, key)).runSyncUnsafe()

      //then
      val existsAfterDeletion = s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe()
      existedBefore shouldBe true
      existsAfterDeletion shouldBe false
    }

    "check if a bucket exists" in {
      //given
      val bucketNameA = nonEmptyString.value()
      val bucketNameB = nonEmptyString.value()

      //and
      s3Resource.use(_.createBucket(bucketNameA)).runSyncUnsafe()

      //when
      val isPresentA = s3Resource.use(_.existsBucket(bucketNameA)).runSyncUnsafe()
      val isPresentB = s3Resource.use(_.existsBucket(bucketNameB)).runSyncUnsafe()

      //then
      isPresentA shouldBe true
      isPresentB shouldBe false
    }

    "list existing buckets" in {
      //given
      val bucketNameA = nonEmptyString.value()
      val _ = nonEmptyString.value()
      val bucketNameC = nonEmptyString.value()

      //and
      val initialBuckets = s3Resource.use(_.listBuckets().toListL).runSyncUnsafe()
        s3Resource.use(_.createBucket(bucketNameA)).runSyncUnsafe()
      s3Resource.use(_.createBucket(bucketNameC)).runSyncUnsafe()

      //when
      val buckets = s3Resource.use(_.listBuckets().toListL).runSyncUnsafe()

      //then
      buckets.size - initialBuckets.size shouldBe 2
      (buckets diff initialBuckets).map(_.name) should contain theSameElementsAs List(bucketNameA, bucketNameC)
    }

    "check if an object exists" in {
      //given
      val prefix = s"test-exists-object/${nonEmptyString.value()}/"
      val key: String = prefix + nonEmptyString.value()

      //and
      s3Resource.use(_.upload(bucketName, key, "dummy content".getBytes())).runSyncUnsafe()

      //when
      val isPresent1 = s3Resource.use(_.existsObject(bucketName, key = key)).runSyncUnsafe()
      val isPresent2 = s3Resource.use(_.existsObject(bucketName, key = "non existing key")).runSyncUnsafe()

      //then
      isPresent1 shouldBe true
      isPresent2 shouldBe false
    }

    "list all objects" in {
      //given
      val n = 1000
      val prefix = s"test-list-all-truncated/${nonEmptyString.value()}/"
      val keys: List[String] =
        Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + nonEmptyString.value() + str)).sample.get
      val contents: List[String] = List.fill(n)(nonEmptyString.value())
      Task
        .sequence(keys.zip(contents).map { case (key, content) => s3Resource.use(_.upload(bucketName, key, content.getBytes())) })
        .runSyncUnsafe()

      //when
      val count = s3Resource.use(_.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(n)).countL).runSyncUnsafe()

      //then
      count shouldBe n
    }

  }

}
