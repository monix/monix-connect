package monix.connect.gcp.storage

import java.io.File
import java.nio.file.{Files, Path}
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, Option => _}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AsyncWordSpec}
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import monix.reactive.Observable
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.apache.commons.io.FileUtils

import scala.concurrent.duration._

class GcsBlobSuite extends AsyncWordSpec with MonixTaskTest with Matchers with BeforeAndAfterAll {

  override implicit val scheduler: Scheduler = Scheduler.io("gcs-blob-suite")
  val storage: Storage = LocalStorageHelper.getOptions.getService
  val dir = new File("gcs/blob-test").toPath
  val genLocalPath = Gen.identifier.map(s => dir.toAbsolutePath.toString + "/" + s)
  val testBucketName = Gen.identifier.sample.get

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(dir.toFile)
    Files.createDirectory(dir)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.beforeAll()
  }

  s"$GcsBlob" should {

    "return true if exists" in {
      val blobPath = Gen.identifier.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      val blob: Blob = storage.create(blobInfo, content)
      val gcsBlob = new GcsBlob(blob)

      gcsBlob.exists().asserting(_ shouldBe true)
    }

    "return delete if exists" in {
      val blobPath = Gen.identifier.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      val blob: Blob = storage.create(blobInfo, content)
      val gcsBlob = new GcsBlob(blob)
      for {
        existedBefore <- gcsBlob.exists()
        deleted <- gcsBlob.delete()
        existsAfterDeletion <- gcsBlob.exists()
      } yield {
        existedBefore shouldBe true
        deleted shouldBe true
        existsAfterDeletion shouldBe false
      }
    }

    "download a small blob in form of observable" in {
      val blobPath = Gen.identifier.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      val blob: Blob = storage.create(blobInfo, content)
      val gcsBlob = new GcsBlob(blob)

      for {
        r <- gcsBlob.download().headL
        exists <- gcsBlob.exists()
      } yield {
        exists shouldBe true
        r shouldBe content
      }
    }

    "download blob from a GcsBlob that resides within a task" in {
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      val gcsStorage = GcsStorage(storage)
      val blob: Task[GcsBlob] = gcsStorage.createBlob("myBucket", "myBlob").memoize

      blob.flatMap(b => Observable.now(content).consumeWith(b.upload())) *>
      Observable.fromTask(blob)
        .flatMap(_.download())
        .headL
        .asserting(_ shouldBe content)
    }

    "download to file" in {
      val blobPath = Gen.identifier.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
      val filePath: Path = new File(genLocalPath.sample.get).toPath
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      val blob: Blob = storage.create(blobInfo, content)
      val gcsBlob = new GcsBlob(blob)

      for {
        _ <- gcsBlob.downloadToFile(filePath) >> Task.sleep(2.seconds)
        exists <- gcsBlob.exists()
        r = Files.readAllBytes(filePath)
      } yield {
        exists shouldBe true
        r shouldBe content
      }
    }

    "upload to the blob" when {

      "it is empty" in {
        val blobPath = Gen.identifier.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
        val blob: Blob = storage.create(blobInfo)
        val gcsBlob = new GcsBlob(blob)
        val content: Array[Byte] = Gen.identifier.sample.get.getBytes()

        for {
          contentBefore <- gcsBlob.download().headOptionL
          _ <- Observable.pure(content).consumeWith(gcsBlob.upload())
          exists <- gcsBlob.exists()
          r <- gcsBlob.download().headL
        } yield {
          exists shouldBe true
          contentBefore.isEmpty shouldBe true
          r shouldBe content
        }
      }

      "it is not empty" in {
        val blobPath = Gen.identifier.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
        val oldContent: Array[Byte] = Gen.identifier.sample.get.getBytes()
        val newContent: Array[Byte] = Gen.identifier.sample.get.getBytes()
        val blob: Blob = storage.create(blobInfo, oldContent)
        val gcsBlob = new GcsBlob(blob)

          for {
            contentBefore <- gcsBlob.download().headOptionL
            actualContent <- Observable.now(newContent).consumeWith(gcsBlob.upload()) >> gcsBlob.download().headL
            exists <- gcsBlob.exists()
          } yield {
            contentBefore.isEmpty shouldBe false
            exists shouldBe true
            actualContent shouldBe newContent
          }
      }

      "the consumed observable is empty" in {
        val blobPath = Gen.identifier.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
        val blob: Blob = storage.create(blobInfo)
        val gcsBlob = new GcsBlob(blob)

        for {
          contentBefore <-gcsBlob.download().headOptionL
          _ <- Observable.pure(Array.emptyByteArray).consumeWith(gcsBlob.upload())
          actualContent <- gcsBlob.download().headOptionL
        } yield {
          contentBefore.isEmpty shouldBe true
          actualContent.isEmpty shouldBe true
        }
      }
    }

    "uploads to the blob from a file" in {
      val blobPath = Gen.identifier.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
      val blob: Blob = storage.create(blobInfo)
      val gcsBlob = new GcsBlob(blob)
      val sourcePath = new File(genLocalPath.sample.get).toPath
      val targetPath = new File(genLocalPath.sample.get).toPath
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      Files.write(sourcePath, content)

      for {
        contentBefore <- gcsBlob.download().headOptionL
        _ <- gcsBlob.uploadFromFile(sourcePath)
        exists <- gcsBlob.exists()
      } yield {
        exists shouldBe true
        contentBefore.isDefined shouldBe false
        gcsBlob.underlying.downloadTo(targetPath)
        val r = Files.readAllBytes(targetPath)
        r shouldBe content
      }
    }

    "return a failed task when uploading from a non existent file" in {
      val blobPath = Gen.identifier.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
      val blob: Blob = storage.create(blobInfo)
      val gcsBlob = new GcsBlob(blob)
      val sourcePath = new File(genLocalPath.sample.get).toPath

      for {
        contentBefore <- gcsBlob.download().headOptionL
        uploadAttempt <- gcsBlob.uploadFromFile(sourcePath).attempt
        actualContent <- gcsBlob.download().headOptionL
      } yield {
        uploadAttempt.isLeft shouldBe true
        contentBefore.isDefined shouldBe false
        actualContent.isDefined shouldBe false
      }
    }

    /** not supported by the [[LocalStorageHelper]]
    "create and lists acls" in {
      //given
      val blobPath = nonEmptyString.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
      val blob: Blob = storage.create(blobInfo)
      val gcsBlob = new GcsBlob(blob)
      val userAcl = Acl.of(new User("user@email.com"), Role.OWNER)
      val groupAcl = Acl.of(new Group("group@email.com"), Role.READER)

      //when
      val r1: Acl = gcsBlob.createAcl(userAcl).runSyncUnsafe()
      val r2: Acl = gcsBlob.createAcl(groupAcl).runSyncUnsafe()
      val l: List[Acl] = gcsBlob.listAcls().toListL.runSyncUnsafe()

      //then
      r1 shouldBe userAcl
      r2 shouldBe groupAcl
      l should contain theSameElementsAs List(userAcl, groupAcl)
    }
     */

  }

}
