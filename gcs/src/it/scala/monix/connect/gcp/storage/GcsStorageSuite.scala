package monix.connect.gcp.storage

import com.google.cloud.storage.Bucket.BucketSourceOption
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{BlobId, BlobInfo, Option => _}
import monix.connect.gcp.storage.configuration.GcsBucketInfo.Locations
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.exceptions.DummyException
import monix.testing.scalatest.MonixTaskTest
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class GcsStorageSuite extends AsyncWordSpec with MonixTaskTest with IdiomaticMockito with Matchers with ArgumentMatchersSugar with BeforeAndAfterAll {

  override implicit val scheduler: Scheduler = Scheduler.io("gcs-storage-suite")
  private val storage = LocalStorageHelper.getOptions.getService
  private val testBucketName = Gen.identifier.sample.get

  s"$GcsStorage" should {

    "create a blob" in {
      val blobName = Gen.identifier.sample.get
      val gcsStorage = GcsStorage(storage)

      gcsStorage.createBlob(testBucketName, blobName).asserting{ createdBlob =>
        createdBlob.underlying.getName shouldBe blobName
        createdBlob.underlying.getBucket shouldBe testBucketName
      }
    }

    "get existing blob from its id " in {
      val blobName = Gen.identifier.sample.get
      val blobInfo: BlobInfo = BlobInfo.newBuilder(BlobId.of(testBucketName, blobName)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      storage.create(blobInfo, content)
      val gcsStorage = GcsStorage(storage)

      gcsStorage.getBlob(testBucketName, blobName).asserting{
        _.isDefined shouldBe true
      }
    }
    
    "return empty when getting non existing blob from its id" in {
      val blobName = Gen.identifier.sample.get
      val gcsStorage = GcsStorage(storage)

      gcsStorage.getBlob(testBucketName, blobName).asserting {
        _.isDefined shouldBe false
      }
    }

    "get exhaustively the list of existing blobs from the the given blob ids" in {
      val blob1 = BlobInfo.newBuilder(BlobId.of(testBucketName, Gen.identifier.sample.get)).build
      val blob2 = BlobInfo.newBuilder(BlobId.of(testBucketName, Gen.identifier.sample.get)).build
      val nonExistingBlob = BlobInfo.newBuilder(BlobId.of(testBucketName, Gen.identifier.sample.get)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      storage.create(blob1, content)
      storage.create(blob2, content)
      val gcsStorage = GcsStorage(storage)

      gcsStorage.getBlobs(List(blob1.getBlobId, blob2.getBlobId, nonExistingBlob.getBlobId)).asserting{
        _.size shouldBe 2
      }
    }

    "list all the blobs under the given bucketName" in {
      val bucketName = Gen.identifier.sample.get
      val blob1 = BlobInfo.newBuilder(BlobId.of(bucketName, Gen.identifier.sample.get)).build
      val blob2 = BlobInfo.newBuilder(BlobId.of(bucketName, Gen.identifier.sample.get)).build
      val content: Array[Byte] = Gen.identifier.sample.get.getBytes()
      val storage = LocalStorageHelper.getOptions.getService //todo check [[LocalStorageHelper]] since removing storage should work equally, but it does not
      storage.create(blob1, content)
      storage.create(blob2, content)
      val gcsStorage = GcsStorage(storage)

      gcsStorage.listBlobs(bucketName).toListL.asserting {
        _.size shouldBe 2
      }
    }

  }

}
