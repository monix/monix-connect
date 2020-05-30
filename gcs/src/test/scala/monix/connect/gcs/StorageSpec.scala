package monix.connect.gcs

import java.util

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Storage.{BucketGetOption, BucketListOption, BucketTargetOption}
import com.google.cloud.storage.{BucketInfo, Bucket => GoogleBucket, Storage => GoogleStorage, Option => _}
import monix.connect.gcs.configuration.BucketConfig
import monix.execution.Scheduler.Implicits.global
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.when
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class StorageSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers {
  val underlying: GoogleStorage = mock[GoogleStorage]
  val storage: Storage = Storage(underlying)
  val bucket: GoogleBucket = mock[GoogleBucket]

  s"$Storage" should {

    "implement an async create bucket operation" in {
      //given
      val bucketTargetOption: BucketTargetOption = mock[BucketTargetOption]
      val bucketInfo: BucketInfo = mock[BucketInfo]
      when(underlying.create(bucketInfo, bucketTargetOption)).thenReturn(bucket)

      //when
      val config = mock[BucketConfig]
      val maybeBucket: Bucket = storage.createBucket(config, bucketTargetOption).runSyncUnsafe()

      //then
      maybeBucket shouldBe a[Bucket]
    }

    "implement an async get bucket operation" that {
      "correctly returns some bucket" in {
        //given
        val bucketGetOption: BucketGetOption = mock[BucketGetOption]
        when(underlying.get("bucket", bucketGetOption)).thenReturn(bucket)

        //when
        val maybeBucket: Option[Bucket] = storage.getBucket("bucket", bucketGetOption).runSyncUnsafe()

        //then
        maybeBucket.isDefined shouldBe true
        maybeBucket.get shouldBe a[Bucket]
      }

      "safely returns none whenever the underlying response was null" in {
        //given
        val bucketGetOption: BucketGetOption = mock[BucketGetOption]
        when(underlying.get("bucket", bucketGetOption)).thenReturn(null)

        //when
        val maybeBucket: Option[Bucket] = storage.getBucket("bucket", bucketGetOption).runSyncUnsafe()

        //then
        maybeBucket.isDefined shouldBe false
      }
    }

    "implement an async list buckets operation" in {
        // given
        val page = mock[Page[GoogleBucket]]
        val bucketListOption: BucketListOption = mock[BucketListOption]
        when(page.iterateAll()).thenReturn(util.Arrays.asList(bucket, bucket, bucket))
        when(underlying.list(bucketListOption)).thenReturn(page)

        //when
        val maybeBuckets: List[Bucket] = storage.listBuckets(bucketListOption).toListL.runSyncUnsafe()

        //then
        maybeBuckets shouldBe a[List[Bucket]]
        maybeBuckets.length shouldBe 3
    }
  }
}