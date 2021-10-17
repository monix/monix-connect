package monix.connect.s3

import monix.connect.aws.auth.MonixAwsConf

import java.io.FileInputStream
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import software.amazon.awssdk.services.s3.model.{ CopyObjectResponse, NoSuchBucketException, NoSuchKeyException, PutObjectResponse}
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec

class S3Suite
  extends AsyncFlatSpec with MonixTaskSpec with Matchers with BeforeAndAfterAll with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"
  override implicit val scheduler = Scheduler.io("multipart-download-observable-suite")

  override def beforeAll(): Unit = {
    super.beforeAll()
    unsafeS3.createBucket(bucketName).attempt.runSyncUnsafe() match {
      case Right(_) => info(s"Created s3 bucket $bucketName")
      case Left(e) => info(s"Failed to create s3 bucket $bucketName with exception: ${e.getMessage}")
    }
  }

  "S3" can "be created from config file" in {
    val s3FromConf = S3.fromConfig
    val (k1, k2) = (Gen.identifier.sample.get, Gen.identifier.sample.get)
    for {
      _ <- s3FromConf.use(_.upload(bucketName, k1, k1.getBytes()))
      _ <- s3FromConf.use(_.upload(bucketName, k2, k2.getBytes()))
      existsK1 <- unsafeS3.existsObject(bucketName, k1)
      existsK2 <- unsafeS3.existsObject(bucketName, k2)
    } yield {
      existsK1 shouldBe true
      existsK2 shouldBe true
    }

  }

  it can "be created from a raw monix aws conf" in {
    val monixAwsConf = MonixAwsConf.load()
    val s3FromConf = monixAwsConf.map(S3.fromConfig).memoizeOnSuccess
    val (k1, k2) = (Gen.identifier.sample.get, Gen.identifier.sample.get)

    s3FromConf.flatMap(resource =>
      resource.use { s3 =>
        for {
          _ <- s3.upload(bucketName, k1, k1.getBytes()) >>
            s3.upload(bucketName, k2, k2.getBytes())
          existsKey1 <- s3.existsObject(bucketName, k1)
          existsKey2 <- s3.existsObject(bucketName, k2)
        } yield {
          //then
          existsKey1 shouldBe true
          existsKey2 shouldBe true
        }
      }
    )
  }

  it can "be created from task monix aws conf" in {
    val monixAwsConf = MonixAwsConf.load().memoizeOnSuccess
    val (k1, k2) = (Gen.identifier.sample.get, Gen.identifier.sample.get)
    for {
      s3FromConf <- Task(S3.fromConfig(monixAwsConf))
      assertion <- s3FromConf.use { s3 =>
        for {
          _ <- s3.upload(bucketName, k1, k1.getBytes()) >>
            s3.upload(bucketName, k2, k2.getBytes())
          existsKey1 <- s3.existsObject(bucketName, k1)
          existsKey2 <- s3.existsObject(bucketName, k2)
        } yield {
          existsKey1 shouldBe true
          existsKey2 shouldBe true
        }
      }
    } yield assertion
  }

  it should "implement upload method" in {
    val key: String = Gen.identifier.sample.get
    val content: String = Gen.alphaUpperStr.sample.get
     for {
       putResponse <- unsafeS3.upload(bucketName, key, content.getBytes())
       s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
     } yield {
       putResponse shouldBe a[PutObjectResponse]
       s3Object shouldBe content.getBytes()
     }
  }

  it can "upload an empty bytearray" in {
    val key: String = Gen.identifier.sample.get
    val content: Array[Byte] = Array.emptyByteArray

    s3Resource.use(_.upload(bucketName, key, content)).map{ putResponse =>
      eventually {
        val s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
        putResponse shouldBe a[PutObjectResponse]
        s3Object shouldBe content
      }
    }
  }

  it can "create an empty object out of an empty chunk" in {
    val key: String = Gen.identifier.sample.get
    val content: Array[Byte] = Array.emptyByteArray
    s3Resource.use(_.upload(bucketName, key, content)).map{ putResponse =>
      eventually {
        val s3Object: Array[Byte] = unsafeDownload(bucketName, key).get
        putResponse shouldBe a[PutObjectResponse]
        s3Object shouldBe content
      }
    }
  }

  it can "download a s3 object as byte array" in {
    val key: String = Gen.identifier.sample.get
    val content: String = Gen.alphaUpperStr.sample.get
    for {
      _ <- unsafeS3.upload(bucketName, key, content.getBytes)
      actualContent <- unsafeS3.download(bucketName, key)
      existsObject <- unsafeS3.existsObject(bucketName, key)
    } yield {
      existsObject shouldBe true
      actualContent shouldBe content.getBytes()
    }
  }

  it can "download a s3 object bigger than 1MB as byte array" in {
    val key: String = Gen.identifier.sample.get
    val inputStream = Task(new FileInputStream(resourceFile("test.csv")))
    val testData: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
        for {
          _ <- testData.consumeWith(unsafeS3.uploadMultipart(bucketName, key))
          existsObject <- unsafeS3.existsObject(bucketName, key)
          actualContent <- unsafeS3.download(bucketName, key)
          expectedArrayByte <- testData.foldLeftL(Array.emptyByteArray)((acc, bytes) => acc ++ bytes)
        } yield {
          existsObject shouldBe true
          actualContent.size shouldBe expectedArrayByte.size
          actualContent shouldBe expectedArrayByte
        }
  }

  it can "download the first n bytes form an object" in {
    //given
    val n = 5
    val key: String = Gen.identifier.sample.get
    val content: String = Gen.identifier.sample.get


    //when
    for {
      _ <- unsafeS3.upload(bucketName, key, content.getBytes)
      partialContent <- unsafeS3.download(bucketName, key, Some(n))
      existsObject <- unsafeS3.existsObject(bucketName, key)
    } yield {
      existsObject shouldBe true
      partialContent shouldBe content.getBytes().take(n)
    }
  }

  it should "fail when downloading with a negative `numberOfBytes`" in {
    val negativeNum = Gen.chooseNum(-10, -1).sample.get
    val key: String = Gen.identifier.sample.get
    unsafeS3.download(bucketName, key, Some(negativeNum)).assertThrows[IllegalArgumentException]
  }

  it should "fail when downloading with a `numberOfBytes` equal to zero" in {
    val chunkSize = 0
    unsafeS3.download("no-bucket", "no-key", Some(chunkSize)).assertThrows[IllegalArgumentException]
  }

  it should "fail when downloading from a non existing key" in {
    val key: String = "non-existing-key"
    unsafeS3.download(bucketName, key).assertThrows[NoSuchKeyException]
  }

  it can "download in multipart" in {

    for {
      key <- Task.from(Gen.identifier)
      content <- Task.from(Gen.identifier)
      _ <- unsafeS3.upload(bucketName, key, content.getBytes)
      existsObject <- unsafeS3.existsObject(bucketName, key)
      actualContent <- unsafeS3.downloadMultipart(bucketName, key, 2).toListL.map(_.flatten.toArray)
    } yield {
      existsObject shouldBe true
      actualContent shouldBe content.getBytes()
    }
  }

  it can "copy an object to a different location within the same bucket" in {
    for {
      sourceKey <- Task.from(genKey)
      content <- Task.from(Gen.identifier).map(_.getBytes)
      destinationKey <- Task.from(genKey)
      _ <- unsafeS3.upload(bucketName, sourceKey, content)
      copyObjectResponse <- unsafeS3.copyObject(bucketName, sourceKey, bucketName, destinationKey)
      actualContent <- unsafeS3.download(bucketName, destinationKey)
    } yield {
      copyObjectResponse shouldBe a[CopyObjectResponse]
      actualContent shouldBe content
    }
  }

  it can "copy an object to a different location in a different bucket" in {
    for {
      sourceKey <- Task.from(genKey)
      content <- Task.from(Gen.identifier).map(_.getBytes)
      _ <- unsafeS3.upload(bucketName, sourceKey, content)
      destinationBucket = genBucketName.sample.get
      destinationKey = genKey.sample.get
      _ <- unsafeS3.createBucket(destinationBucket)
      copyObjectResponse <- unsafeS3.copyObject(bucketName, sourceKey, destinationBucket, destinationKey)
    } yield {
      copyObjectResponse shouldBe a[CopyObjectResponse]
      s3Resource.use(_.download(destinationBucket, destinationKey)).runSyncUnsafe() shouldBe content
    }
  }

  it can "create and delete a bucket" in {
    for {
      bucket <- Task.from(genBucketName)
      _ <- unsafeS3.createBucket(bucket)
      existedBefore <- unsafeS3.existsBucket(bucket)
      _ <- unsafeS3.deleteBucket(bucket)
      existsAfterDeletion <- unsafeS3.existsBucket(bucket)
    } yield {
      existedBefore shouldBe true
      existsAfterDeletion shouldBe false
    }
  }

  it should "fail with `NoSuchBucketException` trying to delete a non existing bucket" in {
    for {
      bucket <- Task.from(genBucketName)
      existedBefore <- unsafeS3.existsBucket(bucket)
      deleteAttempt <- unsafeS3.deleteBucket(bucket).attempt
      existsAfterDeletion <- unsafeS3.existsBucket(bucket)
    } yield {
      existedBefore shouldBe false
      deleteAttempt.isLeft shouldBe true
      deleteAttempt.left.get shouldBe a[NoSuchBucketException]
      existsAfterDeletion shouldBe false
    }
  }

  it can "delete a object" in {
     for {
       key <- Task.from(genKey)
       content <- Task.from(Gen.identifier).map(_.getBytes)
       _ <- unsafeS3.upload(bucketName, key, content)
       existedBeforeDeletion <- unsafeS3.existsObject(bucketName, key)
       _ <- unsafeS3.deleteObject(bucketName, key)
       existedAfterDeletion <- unsafeS3.existsObject(bucketName, key)
     } yield {
       existedBeforeDeletion shouldBe true
       existedAfterDeletion shouldBe false
     }
  }

  it can "check if a bucket exists" in {
    //given
    val bucketNameA = genBucketName.sample.get
    val bucketNameB = genBucketName.sample.get
    for {
      _ <- unsafeS3.createBucket(bucketNameA)
      existsBucketA <- unsafeS3.existsBucket(bucketNameA)
      existsBucketB <- unsafeS3.existsBucket(bucketNameB)
    } yield {
      existsBucketA shouldBe true
      existsBucketB shouldBe false
    }
  }

  it can "list existing buckets" in {
    val bucketName1 = genBucketName.sample.get
    val bucketName2 = genBucketName.sample.get
    for {
      initialBuckets <- unsafeS3.listBuckets().toListL
      _ <- unsafeS3.createBucket(bucketName1)
      _ <- unsafeS3.createBucket(bucketName2)
      finalBuckets <- unsafeS3.listBuckets().toListL
    } yield {
      finalBuckets.size - initialBuckets.size shouldBe 2
      (finalBuckets diff initialBuckets).map(_.name) should contain theSameElementsAs List(bucketName1, bucketName2)
    }
  }

  it can "check if an object exists" in {
    val prefix = s"test-exists-object/${Gen.identifier.sample.get}/"
    val key: String = prefix + genKey.sample.get

    for {
      _ <- unsafeS3.upload(bucketName, key, "dummy content".getBytes())
      isPresent1 <- unsafeS3.existsObject(bucketName, key = key)
      isPresent2 <- unsafeS3.existsObject(bucketName, key =  "non existing key")
    } yield {
      isPresent1 shouldBe true
      isPresent2 shouldBe false
    }
  }

  it can "list all objects" in {
    val n = 1000
    val prefix = s"test-list-all-truncated/${genKey.sample.get}/"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + genKey.sample.get + str)).sample.get
    val contents: List[String] = Gen.listOfN(n, Gen.identifier).sample.get
    for {
      _ <- Task.traverse(keys.zip(contents)){ case (key, content) => unsafeS3.upload(bucketName, key, content.getBytes()) }
      count <- unsafeS3.listObjects(bucketName, prefix = Some(prefix), maxTotalKeys = Some(n)).countL
    } yield {
      count shouldBe n
    }
  }

  it should "list the latest object" in {
    val n = 10
    val prefix = s"test-latest/${genKey.sample.get}"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + str)).sample.get

    S3.fromConfig.use { s3 =>
      for {
        _ <- Task.traverse(keys) { key => s3.upload(bucketName, key, "dummyContent".getBytes()) }
        latest <- s3.listLatestObject(bucketName, prefix = Some(prefix))
        latestFive <- s3.listLatestNObjects(bucketName, 5, prefix = Some(prefix)).toListL
        all <- s3.listObjects(bucketName, prefix = Some(prefix)).toListL
      } yield {
        all.map(_.key()) should contain theSameElementsAs keys
        latest.map(_.key()) shouldBe keys.lastOption
        latest shouldBe all.sortBy(_.lastModified()).reverse.headOption
        latestFive should contain theSameElementsAs all.sortBy(_.lastModified()).reverse.take(5)
      }
    }.assertNoException
  }

    it should "list the oldest object" in {
    val n = 20
    val prefix = s"test-oldest/${genKey.sample.get}"
    val keys: List[String] =
      Gen.listOfN(n, Gen.alphaLowerStr.map(str => prefix + str)).sample.get

    S3.fromConfig.use { s3 =>
      for {
        _ <- Task
          .traverse(keys) { key => s3.upload(bucketName, key, "dummyContent".getBytes()) }
        oldest <- s3.listOldestObject(bucketName, prefix = Some(prefix))
        oldestFive <- s3.listOldestNObjects(bucketName, 5, prefix = Some(prefix)).toListL
        all <- s3.listObjects(bucketName, prefix = Some(prefix)).toListL
      } yield {
        all.map(_.key()) should contain theSameElementsAs keys
        oldest.map(_.key()) shouldBe keys.headOption
        oldest shouldBe all.sortBy(_.lastModified()).headOption
        oldestFive should contain theSameElementsAs all.sortBy(_.lastModified()).take(5)
      }
    }.assertNoException
  }

  it should "return with less than requested for" in {
    //given
    val prefix = s"test-less-than-requested/${genKey.sample.get}"
    val keys: List[String] =
      Gen.listOfN(10, Gen.alphaLowerStr.map(str => prefix + str)).sample.get

    //when
    S3.fromConfig.use { s3 =>
      for {
        _ <- Task
          .traverse(keys) { key => s3.upload(bucketName, key, "dummyContent".getBytes()) }
        all <- s3.listObjects(bucketName, prefix = Some(prefix)).toListL
        listNObjects <- s3.listOldestNObjects(bucketName, all.size + 10, prefix = Some(prefix)).toListL
      } yield {
        all.size shouldBe listNObjects.size
      }
    }.assertNoException
  }

  "A real usage" should "reuse the resource evaluation" in {
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

    S3.fromConfig.use(runS3App).asserting(_ shouldBe content.getBytes)
  }


}
