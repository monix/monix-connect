package monix.connect.gcp.storage

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{ BlobId, BlobInfo, Option => _}
import monix.execution.Scheduler.Implicits.global
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class GcsStorageSuite extends AnyWordSpecLike with IdiomaticMockito with Matchers with ArgumentMatchersSugar with BeforeAndAfterAll {

  val storage = LocalStorageHelper.getOptions.getService
  val testBucketName = Gen.identifier.sample.get


  s"${GcsStorage}" should {

    "create a blob" in {
      //given two not and existing blobs
      val blobName = Gen.identifier.sample.get
      val gcsStorage = GcsStorage(storage)

      //when
      val t = gcsStorage.createBlob(testBucketName, blobName)
      val createdBlob = t.runSyncUnsafe()

      //then
      createdBlob.underlying.getName shouldBe blobName
      createdBlob.underlying.getBucket shouldBe testBucketName
    }

    "get existing blob from its id " in {
      //given
      val blobName = Gen.identifier.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobName)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      storage.create(blobInfo, content)
      val gcsStorage = GcsStorage(storage)

      //when
      val t = gcsStorage.getBlob(testBucketName, blobName)
      val gcsBlob: Option[GcsBlob] = t.runSyncUnsafe()

      //then
      gcsBlob.isDefined shouldBe true
    }

    "return empty when getting non existing blob from its id" in {
      //given
      val blobName = Gen.identifier.sample.get
      val gcsStorage = GcsStorage(storage)

      //when
      val t = gcsStorage.getBlob(testBucketName, blobName)
      val gcsBlob: Option[GcsBlob] = t.runSyncUnsafe()

      //then
      gcsBlob.isDefined shouldBe false
    }

    "get exhaustively the list of existing blobs from the the given blob ids" in {
      //given two not and existing blobs
      val blob1 = BlobInfo.newBuilder(BlobId.of(testBucketName, Gen.identifier.sample.get)).build
      val blob2 = BlobInfo.newBuilder(BlobId.of(testBucketName, Gen.identifier.sample.get)).build
      val nonExistingBlob = BlobInfo.newBuilder(BlobId.of(testBucketName, Gen.identifier.sample.get)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      storage.create(blob1, content)
      storage.create(blob2, content)
      val gcsStorage = GcsStorage(storage)

      //when
      val t = gcsStorage.getBlobs(List(blob1.getBlobId, blob2.getBlobId, nonExistingBlob.getBlobId))
      val gcsBlob: List[GcsBlob] = t.runSyncUnsafe()

      //then
      gcsBlob.size shouldBe 2
    }

    "list all the blobs under the given bucketName" in {
      //given
      val bucketName = Gen.identifier.sample.get
      val blob1 = BlobInfo.newBuilder(BlobId.of(bucketName, Gen.identifier.sample.get)).build
      val blob2 = BlobInfo.newBuilder(BlobId.of(bucketName, Gen.identifier.sample.get)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      val storage = LocalStorageHelper.getOptions.getService //todo check [[LocalStorageHelper]] since removing storage should work equally, but it does not
      storage.create(blob1, content)
      storage.create(blob2, content)
      val gcsStorage = GcsStorage(storage)

      //when
      val ob = gcsStorage.listBlobs(bucketName)
      val gcsBlob: List[GcsBlob] = ob.toListL.runSyncUnsafe()

      //then
      gcsBlob.size shouldBe 2
    }

  }

}
