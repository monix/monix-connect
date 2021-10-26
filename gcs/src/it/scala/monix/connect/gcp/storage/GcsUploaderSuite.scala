package monix.connect.gcp.storage

import java.io.File
import java.nio.file.Files
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Option => _}
import monix.connect.gcp.storage.components.GcsUploader
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskSpec
import org.apache.commons.io.FileUtils
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpecLike, AsyncWordSpec}

class GcsUploaderSuite extends AsyncWordSpec with MonixTaskSpec with IdiomaticMockito with Matchers with ArgumentMatchersSugar with BeforeAndAfterAll {

  val storage = LocalStorageHelper.getOptions.getService
  val dir = new File("gcs/uploader-test").toPath
  val genLocalPath = Gen.identifier.map(s => dir.toAbsolutePath.toString + "/" + s)
  val testBucketName = Gen.identifier.sample.get
  override implicit val scheduler: Scheduler = Scheduler.io("gcs-storage-suite")

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(dir.toFile)
    Files.createDirectory(dir)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.beforeAll()

  }

  s"$GcsUploader consumer implementation" should {

    "upload to specified blob" when {

      "it is empty" in {
        val blobPath = Gen.identifier.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
        val blob: Blob = storage.create(blobInfo)
        val gcsBlob = new GcsBlob(blob)
        val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
        val uploader = GcsUploader(GcsStorage(storage), blobInfo)

        for {
          contentBefore <- gcsBlob.download().headOptionL
          _ <- Observable.pure(content).consumeWith(uploader)
          existsBlob <- gcsBlob.exists()
          actualContent <- gcsBlob.download().headL
        } yield {
          existsBlob shouldBe true
          contentBefore.isEmpty shouldBe true
          actualContent shouldBe content
        }
      }

      "it is not empty" in {
        val blobPath = Gen.identifier.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
        val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
        val blob: Blob = storage.create(blobInfo, content)
        val gcsBlob = new GcsBlob(blob)
        val uploader = GcsUploader(GcsStorage(storage), blobInfo)

        for {
          contentBefore <- gcsBlob.download().headOptionL
          _ <- Observable.now(content).consumeWith(uploader)
          exists <- gcsBlob.exists()
          actualContent <- gcsBlob.download().headL
        } yield {
          exists shouldBe true
          contentBefore.isEmpty shouldBe false
          actualContent shouldBe content
        }
      }

      "the consumed observable is empty" in {
        val blobName = Gen.identifier.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobName)).build
        val blob: Blob = storage.create(blobInfo)
        val gcsBlob = new GcsBlob(blob)
        val uploader = GcsUploader(GcsStorage(storage), blobInfo)

        for {
          contentBefore <- gcsBlob.download().headOptionL
          _ <- Observable.pure(Array.emptyByteArray).consumeWith(uploader)
          actualContent <- gcsBlob.download().headOptionL
        } yield {
          contentBefore.isEmpty shouldBe true
          actualContent.isEmpty shouldBe true
        }
      }
    }

  }

}
