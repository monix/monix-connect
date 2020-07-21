package monix.connect.gcp.storage

import java.io.File
import java.nio.file.Files

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Option => _}
import monix.connect.gcp.storage.components.GcsUploader
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.commons.io.FileUtils
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class GcsUploaderSuite extends AnyWordSpecLike with IdiomaticMockito with Matchers with ArgumentMatchersSugar with BeforeAndAfterAll {

  val storage = LocalStorageHelper.getOptions.getService
  val dir = new File("gcs/tmp").toPath
  val nonEmptyString: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "test-" + chars.mkString.take(20))
  val genLocalPath = nonEmptyString.map(s => dir.toAbsolutePath.toString + "/" + s)
  val testBucketName = nonEmptyString.sample.get

  override def beforeAll(): Unit = {
    FileUtils.deleteDirectory(dir.toFile)
    Files.createDirectory(dir)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.beforeAll()
  }

  s"${GcsUploader} consumer implementation" should {

    "upload to specified blob" when {

      "it is empty" in {
        //given
        val blobPath = nonEmptyString.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
        val blob: Blob = storage.create(blobInfo)
        val gcsBlob = new GcsBlob(blob)
        val content: Array[Byte] = nonEmptyString.sample.get.getBytes()
        val uploader = GcsUploader(GcsStorage(storage), blobInfo)

        //when
        val downloader: Observable[Array[Byte]] = gcsBlob.download()
        val contentBefore: Option[Array[Byte]] = downloader.headOptionL.runSyncUnsafe()
        Observable.pure(content).consumeWith(uploader).runSyncUnsafe()

        //then
        val exists = gcsBlob.exists().runSyncUnsafe()
        val r: Array[Byte] = downloader.headL.runSyncUnsafe()
        exists shouldBe true
        contentBefore.isEmpty shouldBe true
        r shouldBe content
      }

      "it is not empty" in {
        //given
        val blobPath = nonEmptyString.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobPath)).build
        val content: Array[Byte] = nonEmptyString.sample.get.getBytes()
        val blob: Blob = storage.create(blobInfo, content)
        val gcsBlob = new GcsBlob(blob)
        val uploader = GcsUploader(GcsStorage(storage), blobInfo)


        //when
        val downloader: Observable[Array[Byte]] = gcsBlob.download()
        val contentBefore: Option[Array[Byte]] = downloader.headOptionL.runSyncUnsafe()
        Observable.now(content).consumeWith(uploader).runSyncUnsafe()

        //then
        val exists = gcsBlob.exists().runSyncUnsafe()
        val r: Array[Byte] = downloader.headL.runSyncUnsafe()
        exists shouldBe true
        contentBefore.isEmpty shouldBe false
        r shouldBe content
      }

      "the consumed observable is empty" in {
        //given
        val blobName = nonEmptyString.sample.get
        val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobName)).build
        val blob: Blob = storage.create(blobInfo)
        val gcsBlob = new GcsBlob(blob)
        val uploader = GcsUploader(GcsStorage(storage), blobInfo)

        //when
        val downloader: Observable[Array[Byte]] = gcsBlob.download()
        val contentBefore: Option[Array[Byte]] = downloader.headOptionL.runSyncUnsafe()
        Observable.pure(Array.emptyByteArray).consumeWith(uploader).runSyncUnsafe()

        //then
        val r: Option[Array[Byte]] = downloader.headOptionL.runSyncUnsafe()
        contentBefore.isEmpty shouldBe true
        r.isEmpty shouldBe true
      }
    }

  }

}
