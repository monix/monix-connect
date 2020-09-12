package monix.connect.s3

import java.io.FileInputStream

import monix.eval.Task
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadResponse, CopyObjectResponse, CreateBucketRequest, NoSuchBucketException, NoSuchKeyException, PutObjectResponse, S3Object}
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import Thread.sleep

import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class S3ITest
  extends AnyWordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"

  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(Task.from(s3AsyncClient.createBucket(CreateBucketRequest.builder().bucket(bucketName).build()))) match {
      case Success(_) => info(s"Created S3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create S3 bucket ${bucketName} with exception: ${e.getMessage}")
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
          val t: Task[PutObjectResponse] = S3.upload(bucketName, key, content.getBytes()).asyncBoundary

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
          val t: Task[PutObjectResponse] = S3.upload(bucketName, key, content.getBytes())

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
          val t: Task[PutObjectResponse] = S3.upload(bucketName, key, content)

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
          val t: Task[PutObjectResponse] = S3.upload(bucketName, key, content)

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
        S3.multipartUpload(bucketName, key)
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
      sleep(300)

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
      val _ = ob.consumeWith(S3.multipartUpload(bucketName, key)).runSyncUnsafe()

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

    "downloadMultipart from a non existin object returns an empty byte array" in {
      //given
      val key: String = "non/existing/key"

      //when
      val result: Array[Byte] = S3.downloadMultipart(bucketName, key, 1).toListL.map(_.flatten.toArray).runSyncUnsafe()

      //then
      result shouldBe Array.emptyByteArray
      S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe false
      result shouldBe a[Array[Byte]]
    }

    "downloading in multipart from a non existing bucket object returns failure" in {
      //given
      val bucket: String = "non-existing-bucket"
      val key: String = "non/existing/key"

      //when
      val f = S3.downloadMultipart(bucket, key, 1).toListL.map(_.flatten.toArray).runToFuture(global)
      sleep(300)

      //then
      f.value.get shouldBe a[Failure[NoSuchBucketException]]
      S3.existsObject(bucket, key).runSyncUnsafe() shouldBe false
    }

  }

  it should {

    "copy an object to a different location within the same bucket" in {
      //given
      val sourceKey = nonEmptyString.value()
      val content = nonEmptyString.value().getBytes()
      S3.upload(bucketName, sourceKey, content).runSyncUnsafe()

      //and
      val destinationKey = nonEmptyString.value()

      //when
      val copyObjectResponse = S3.copyObject(bucketName, sourceKey, bucketName, destinationKey).runSyncUnsafe()

      //then
      copyObjectResponse shouldBe a[CopyObjectResponse]
      S3.download(bucketName, destinationKey).runSyncUnsafe() shouldBe content
    }

    "copy an object to a different location in a different bucket" in {
      //given
      val sourceKey = nonEmptyString.value()
      val content = nonEmptyString.value().getBytes()
      S3.upload(bucketName, sourceKey, content).runSyncUnsafe()

      //and
      val destinationBucket = nonEmptyString.value()
      val destinationKey = nonEmptyString.value()
      S3.createBucket(destinationBucket)

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
        val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
        val content: Array[Byte] = Gen.alphaUpperStr.sample.get.getBytes
        val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
          S3.multipartUpload(bucketName, key)
        val ob = Observable.pure(content)

        //when
        val t: Task[CompleteMultipartUploadResponse] = ob.consumeWith(consumer)

        //then
        whenReady(t.runToFuture) { completeMultipartUpload =>
          val s3Object: Array[Byte] = download(bucketName, key).get
          S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          s3Object shouldBe content
        }
      }

      "multiple chunks (of less than minimum size) are passed" in {
        //given
        val key: String = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
        val chunks: List[Array[Byte]] = Gen.listOfN(10, Gen.alphaUpperStr).map(_.map(_.getBytes)).sample.get
        val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
          S3.multipartUpload(bucketName, key)
        val ob: Observable[Array[Byte]] = Observable.fromIterable(chunks)

        //when
        val t: Task[CompleteMultipartUploadResponse] = ob.consumeWith(consumer)

        //then
        whenReady(t.runToFuture) { completeMultipartUpload =>
          eventually {
            val s3Object: Array[Byte] = download(bucketName, key).get
            S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
            completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
            s3Object shouldBe chunks.flatten
          }
        }
      }

      "a single chunk of size (1MB)" in {
        //given
        val key = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
        val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
        val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
        val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
          S3.multipartUpload(bucketName, key)

        //when
        val t: Task[CompleteMultipartUploadResponse] = ob.consumeWith(consumer)

        //then
        val expectedArrayByte = ob.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL.runSyncUnsafe()
        whenReady(t.runToFuture) { completeMultipartUpload =>
          val s3Object: Array[Byte] = download(bucketName, key).get
          S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
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
        val consumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] =
          S3.multipartUpload(bucketName, key)

        //when
        val t: Task[CompleteMultipartUploadResponse] = ob.consumeWith(consumer)

        //then
        val expectedArrayByte = ob.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL.runSyncUnsafe()
        val completeMultipartUpload = t.runSyncUnsafe()
        eventually {
          val s3Object: Array[Byte] = download(bucketName, key).get
          S3.existsObject(bucketName, key).runSyncUnsafe() shouldBe true
          completeMultipartUpload shouldBe a[CompleteMultipartUploadResponse]
          s3Object shouldBe expectedArrayByte

        }
      }
    }
  }

  it should {

    "create and delete a bucket" in {
      //given
      val bucket = nonEmptyString.value()
      S3.createBucket(bucket).runSyncUnsafe()
      val existedBefore = S3.existsBucket(bucket).runSyncUnsafe()

      //when
      S3.deleteBucket(bucket).runSyncUnsafe()

      //then
      val existsAfterDeletion = S3.existsBucket(bucket).runSyncUnsafe()
      existedBefore shouldBe true
      existsAfterDeletion shouldBe false
    }

    "delete returns NoSuchBucketException when the bucket did not exist" in {
      //given
      val bucket = nonEmptyString.value()
      val existedBefore = S3.existsBucket(bucket).runSyncUnsafe()

      //when
      val f = S3.deleteBucket(bucket).runToFuture(global)
      sleep(300)

      //then
      f.value.get shouldBe a[Failure[NoSuchBucketException]]
      val existsAfterDeletion = S3.existsBucket(bucket).runSyncUnsafe()
      existedBefore shouldBe false
      existsAfterDeletion shouldBe false
    }

    "delete a object" in {
      //given
      val key = nonEmptyString.value()
      val content = nonEmptyString.value().getBytes()
      S3.upload(bucketName, key, content).runSyncUnsafe()
      val existedBefore = S3.existsObject(bucketName, key).runSyncUnsafe()

      //when
      S3.deleteObject(bucketName, key).runSyncUnsafe()

      //then
      val existsAfterDeletion = S3.existsObject(bucketName, key).runSyncUnsafe()
      existedBefore shouldBe true
      existsAfterDeletion shouldBe false
    }

    "check if a bucket exists" in {
      //given
      val bucketNameA = nonEmptyString.value()
      val bucketNameB = nonEmptyString.value()

      //and
      S3.createBucket(bucketNameA).runSyncUnsafe()

      //when
      val isPresentA = S3.existsBucket(bucketNameA).runSyncUnsafe()
      val isPresentB = S3.existsBucket(bucketNameB).runSyncUnsafe()

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
      val initialBuckets = S3.listBuckets().toListL.runSyncUnsafe()
      S3.createBucket(bucketNameA).runSyncUnsafe()
      S3.createBucket(bucketNameC).runSyncUnsafe()

      //when
      val buckets = S3.listBuckets().toListL.runSyncUnsafe()

      //then
      buckets.size - initialBuckets.size shouldBe 2
      (buckets diff initialBuckets).map(_.name) should contain theSameElementsAs List(bucketNameA, bucketNameC)
    }

    "check if an object exists" in {
      //given
      val prefix = s"test-exists-object/${nonEmptyString.value()}/"
      val key: String = prefix + nonEmptyString.value()

      //and
      S3.upload(bucketName, key, "dummy content".getBytes()).runSyncUnsafe()

      //when
      val isPresent1 = S3.existsObject(bucketName, key = key).runSyncUnsafe()
      val isPresent2 = S3.existsObject(bucketName, key = "non existing key").runSyncUnsafe()

      //then
      isPresent1 shouldBe true
      isPresent2 shouldBe false
    }

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

    "list all objects using the continuation token" in {
      //given
      val n = 2000
      val prefix = s"test-list-all-truncated/${nonEmptyString.value()}/"
      val keys: List[String] =
        Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + nonEmptyString.value() + str)).sample.get
      val contents: List[String] = List.fill(n)(nonEmptyString.value())
      Task
        .sequence(keys.zip(contents).map { case (key, content) => S3.upload(bucketName, key, content.getBytes()) })
        .runSyncUnsafe()

      //when
      val s3Objects = S3.listObjects(bucketName, prefix = Some(prefix)).toListL.runSyncUnsafe()

      //then
      s3Objects.size shouldBe n
      s3Objects.map(_.key) should contain theSameElementsAs keys
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

    "list objects is resilient to failures" in {
      //given/when
      val f = S3.listObjects("no-existing-bucket", prefix = Some("prefix")).toListL.runToFuture(global)
      sleep(300)

      //then
      f.value.get shouldBe a[Failure[NoSuchBucketException]]
    }

  }

}
