package monix.connect.s3

import java.io.FileInputStream

import monix.eval.Task
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadResponse, CopyObjectResponse, NoSuchBucketException, NoSuchKeyException, PutObjectResponse}
import org.scalatest.matchers.should.Matchers
import Thread.sleep

import scala.concurrent.duration._
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class S3Suite
  extends AnyFlatSpec with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"
  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(s3Resource.use(_.createBucket(bucketName)).runSyncUnsafe()) match {
      case Success(_) => info(s"Created s3 bucket ${bucketName} ")
      case Failure(e) => info(s"Failed to create s3 bucket ${bucketName} with exception: ${e.getMessage}")
    }
  }

  s"${S3}" can "be created from config files" in {
    //given
    val s3FromConf = S3.fromConfig
    val (k1, k2) = (Gen.identifier.sample.get, Gen.identifier.sample.get)

    //when
    s3FromConf.use(s3 => s3.upload(bucketName, k1, k1.getBytes())).runSyncUnsafe()
    s3FromConf.use(s3 => s3.upload(bucketName, k2, k2.getBytes())).runSyncUnsafe()

    //then
    val (exists1, exists2) = s3FromConf.use(s3 => Task.parZip2(s3.existsObject(bucketName, k1), s3.existsObject(bucketName, k2))).runSyncUnsafe()
    exists1 shouldBe true
    exists2 shouldBe true
  }

  it should "implement upload method" in {
    //given
    val key: String = Gen.identifier.sample.get
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

  it can "upload an empty bytearray" in {
    //given
    val key: String = Gen.identifier.sample.get
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

  it can "create an empty object out of an empty chunk" in {
    //given
    val key: String = Gen.identifier.sample.get
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

  it can "download a s3 object as byte array" in {
    //given
    val key: String = Gen.identifier.sample.get
    val content: String = Gen.alphaUpperStr.sample.get
    s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()

    //when
    val t: Task[Array[Byte]] = s3Resource.use(_.download(bucketName, key))

    //then
    whenReady(t.runToFuture) { actualContent: Array[Byte] =>
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      actualContent shouldBe content.getBytes()
    }
  }

  it can "download a s3 object bigger than 1MB as byte array" in {
    //given
    val key: String = Gen.identifier.sample.get
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
      actualContent.size shouldBe expectedArrayByte.size
      actualContent shouldBe expectedArrayByte
    }
  }

  it can "download the first n bytes form an object" in {
    //given
    val n = 5
    val key: String = Gen.identifier.sample.get
    val content: String = Gen.identifier.sample.get
    s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()

    //when
    val t: Task[Array[Byte]] = s3Resource.use(_.download(bucketName, key, Some(n)))

    //then
    whenReady(t.runToFuture) { partialContent: Array[Byte] =>
      s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
      partialContent shouldBe content.getBytes().take(n)
    }
  }

  it should "fail when downloading with a negative `numberOfBytes`" in {
    //given
    val negativeNum = Gen.chooseNum(-10, -1).sample.get
    val key: String = Gen.identifier.sample.get

    //when
    val f = s3Resource.use(_.download(bucketName, key, Some(negativeNum))).runToFuture

    //then
    Await.ready(f, 3.seconds)
    f.value.get.isFailure shouldBe true
  }

  it should "fail when downloading with a `numberOfBytes` equal to zero" in {
    //given
    val chunkSize = 0

    //when
    val f = s3Resource.use(_.download("no-bucket", "no-key", Some(chunkSize)))
      .runToFuture

    //then
    Await.ready(f, 3.seconds)
    f.value.get.isFailure shouldBe true
  }

  it should "fail when downloading from a non existing key" in {
    //given
    val key: String = "non-existing-key"

    //when
    val f: Future[Array[Byte]] = s3Resource.use(_.download(bucketName, key)).runToFuture(global)
    sleep(400)

    //then
    f.value.get.isFailure shouldBe true
    f.value.get.failed.get shouldBe a[NoSuchKeyException]
  }

  it can "download in multipart" in {
    //given
    val key: String = Gen.identifier.sample.get
    val content: String = Gen.identifier.sample.get
    s3Resource.use(_.upload(bucketName, key, content.getBytes)).runSyncUnsafe()

    //when
    val actualContent: Array[Byte] =
      s3Resource.use(_.downloadMultipart(bucketName, key, 2).toListL).map(_.flatten.toArray).runSyncUnsafe()

    //then
    s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe() shouldBe true
    actualContent shouldBe content.getBytes()
  }

  it can "copy an object to a different location within the same bucket" in {
    //given
    val sourceKey = genKey.sample.get
    val content = Gen.identifier.sample.get.getBytes()
    s3Resource.use(_.upload(bucketName, sourceKey, content)).runSyncUnsafe()

    //and
    val destinationKey = genKey.sample.get

    //when
    val copyObjectResponse =
      s3Resource.use(_.copyObject(bucketName, sourceKey, bucketName, destinationKey)).runSyncUnsafe()

    //then
    copyObjectResponse shouldBe a[CopyObjectResponse]
    s3Resource.use(_.download(bucketName, destinationKey)).runSyncUnsafe() shouldBe content
  }

  it can "copy an object to a different location in a different bucket" in {
    //given
    val sourceKey = genKey.sample.get
    val content = Gen.identifier.sample.get.getBytes()
    s3Resource.use(_.upload(bucketName, sourceKey, content)).runSyncUnsafe()

    //and
    val destinationBucket = genBucketName.sample.get
    val destinationKey = genKey.sample.get
    s3Resource.use(_.createBucket(destinationBucket)).runSyncUnsafe()

    //when
    val copyObjectResponse =
      s3Resource.use(_.copyObject(bucketName, sourceKey, destinationBucket, destinationKey)).runSyncUnsafe()

    //then
    copyObjectResponse shouldBe a[CopyObjectResponse]
    s3Resource.use(_.download(destinationBucket, destinationKey)).runSyncUnsafe() shouldBe content
  }

  it can "create and delete a bucket" in {
    //given
    val bucket = genBucketName.sample.get
    s3Resource.use(_.createBucket(bucket)).runSyncUnsafe()
    val existedBefore = s3Resource.use(_.existsBucket(bucket)).runSyncUnsafe()

    //when
    s3Resource.use(_.deleteBucket(bucket)).runSyncUnsafe()

    //then
    val existsAfterDeletion = s3Resource.use(_.existsBucket(bucket)).runSyncUnsafe()
    existedBefore shouldBe true
    existsAfterDeletion shouldBe false
  }

  it should "fail with `NoSuchBucketException` trying to delete a non existing bucket" in {
    //given
    val bucket = genBucketName.sample.get
    val existedBefore = s3Resource.use(_.existsBucket(bucket)).runSyncUnsafe()

    //when
    val f = s3Resource.use(_.deleteBucket(bucket)).runToFuture(global)
    sleep(400)

    //then
    f.value.get.isFailure shouldBe true
    f.value.get.failed.get shouldBe a[NoSuchBucketException]
    val existsAfterDeletion = s3Resource.use(_.existsBucket(bucket)).runSyncUnsafe()
    existedBefore shouldBe false
    existsAfterDeletion shouldBe false
  }

  it can "delete a object" in {
    //given
    val key = genKey.sample.get
    val content = Gen.identifier.sample.get.getBytes()
    s3Resource.use(_.upload(bucketName, key, content)).runSyncUnsafe()
    val existedBefore = s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe()

    //when
    s3Resource.use(_.deleteObject(bucketName, key)).runSyncUnsafe()

    //then
    val existsAfterDeletion = s3Resource.use(_.existsObject(bucketName, key)).runSyncUnsafe()
    existedBefore shouldBe true
    existsAfterDeletion shouldBe false
  }

  it can "check if a bucket exists" in {
    //given
    val bucketNameA = genBucketName.sample.get
    val bucketNameB = genBucketName.sample.get

    //and
    s3Resource.use(_.createBucket(bucketNameA)).runSyncUnsafe()

    //when
    val isPresentA = s3Resource.use(_.existsBucket(bucketNameA)).runSyncUnsafe()
    val isPresentB = s3Resource.use(_.existsBucket(bucketNameB)).runSyncUnsafe()

    //then
    isPresentA shouldBe true
    isPresentB shouldBe false
  }

  it can "list existing buckets" in {
    //given
    val bucketName1 = genBucketName.sample.get
    val bucketName2 = genBucketName.sample.get

    //and
    val initialBuckets = s3Resource.use(_.listBuckets().toListL).runSyncUnsafe()
    s3Resource.use(_.createBucket(bucketName1)).runSyncUnsafe()
    s3Resource.use(_.createBucket(bucketName2)).runSyncUnsafe()

    //when
    val buckets = s3Resource.use(_.listBuckets().toListL).runSyncUnsafe()

    //then
    buckets.size - initialBuckets.size shouldBe 2
    (buckets diff initialBuckets).map(_.name) should contain theSameElementsAs List(bucketName1, bucketName2)
  }

  it can "check if an object exists" in {
    //given
    val prefix = s"test-exists-object/${Gen.identifier.sample.get}/"
    val key: String = prefix + genKey.sample.get

    //and
    s3Resource.use(_.upload(bucketName, key, "dummy content".getBytes())).runSyncUnsafe()

    //when
    val isPresent1 = s3Resource.use(_.existsObject(bucketName, key = key)).runSyncUnsafe()
    val isPresent2 = s3Resource.use(_.existsObject(bucketName, key = "non existing key")).runSyncUnsafe()

    //then
    isPresent1 shouldBe true
    isPresent2 shouldBe false
  }

  it can "list all objects" in {
    //given
    val n = 1000
    val prefix = s"test-list-all-truncated/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents: List[String] = Gen.listOfN(n, Gen.identifier).sample.get
    Task
      .traverse(keys.zip(contents)){ case (key, content) => s3Resource.use(_.upload(bucketName, key, content.getBytes())) }
      .runSyncUnsafe()

    //when
    val count =
      s3Resource.use(_.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(n)).countL).runSyncUnsafe()

    //then
    count shouldBe n
  }

  it should "return with latest" in {
    //given
    val n = 1010
    val prefix = s"test-latest/${genKey.sample.get}"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents: List[String] = List.fill(n)(genKey.sample.get)
    Task
      .traverse(keys.zip(contents)){ case (key, content) => S3.fromConfig.use(_.upload(bucketName, key, content.getBytes())) }
      .runSyncUnsafe()

    //when
    val latest = S3.fromConfig.use(s3 => s3.listLatestObject(bucketName, prefix = Some(prefix))).runSyncUnsafe()
    val latestList = S3.fromConfig.use(s3 => s3.listObjects(bucketName, prefix = Some(prefix)).toListL).runSyncUnsafe() 
    
    //then
    Some(latestList.sortBy(_.lastModified()).reverse.head) shouldBe latest
  }

  it should "return with oldest" in {
    //given
    val prefix = s"test-latest/"
 
    //when
    val oldest = S3.fromConfig.use(s3 => s3.listOldestObject(bucketName, prefix = Some(prefix))).runSyncUnsafe()
    val oldestList = S3.fromConfig.use(s3 => s3.listObjects(bucketName, prefix = Some(prefix)).toListL).runSyncUnsafe()

    //then
    Some(oldestList.sortBy(_.lastModified()).head) shouldBe oldest
  }

  it should "return with latest five" in {
    //given
    val prefix = s"test-latest/"

    //when
    val latest = S3.fromConfig.use(s3 => s3.listLatestNObjects(bucketName, 5,prefix = Some(prefix)).toListL).runSyncUnsafe()
    val latestList = S3.fromConfig.use(s3 => s3.listObjects(bucketName, prefix = Some(prefix)).toListL).runSyncUnsafe()

    //then
    latestList.sortBy(_.lastModified()).reverse.take(5) shouldBe latest
  }

  it should "return with oldest five" in {
    //given
    val prefix = s"test-latest/"

    //when
    val oldest = S3.fromConfig.use(s3 => s3.listOldestNObjects(bucketName, 5,prefix = Some(prefix)).toListL).runSyncUnsafe()
    val oldestList = S3.fromConfig.use(s3 => s3.listObjects(bucketName, prefix = Some(prefix)).toListL).runSyncUnsafe()

    //then
    oldestList.sortBy(_.lastModified()).take(5) shouldBe oldest
  }

  it should "return with less than asked for" in {

  val prefix = s"test-latest/"

  val actual = S3.fromConfig.use(s3 => s3.listObjects(bucketName, prefix = Some(prefix)).toListL).runSyncUnsafe()
  val wanted = S3.fromConfig.use(s3 => s3.listOldestNObjects(bucketName, actual.size + 10, prefix = Some(prefix)).toListL).runSyncUnsafe()

  wanted.size shouldBe actual.size
  
  }

  "A good real example" should "reuse the resource evaluation" in {

    val bucket = genBucketName.sample.get
    val key = "my-key"
    val content = "my-content"

    def runS3App(s3: S3): Task[Array[Byte]] = {
      for {
        _ <- s3.createBucket(bucket)
        _ <- s3.upload(bucket, key, content.getBytes)
        existsObject <- s3.existsObject(bucket, key)
        download <- {
          if(existsObject) s3.download(bucket, key)
          else Task.raiseError(NoSuchKeyException.builder().build())
        }
      } yield download
    }

    val f = S3.fromConfig.use(s3 => runS3App(s3)).runToFuture

    val result = Await.result(f, 3.seconds)
    result shouldBe content.getBytes
  }


}
