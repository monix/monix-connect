package monix.connect.gcp.storage

import java.util

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Storage.{BucketGetOption, BucketListOption}
import com.google.cloud.storage.{Bucket => GoogleBucket, BucketInfo => GoogleBucketInfo, Storage => GoogleStorage, Option => _}
import monix.connect.gcp.storage.configuration.GcsBucketInfo.Locations
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito.{times, verify}
import org.mockito.MockitoSugar.when
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class GcsStorageSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers with ArgumentMatchersSugar {
  val underlying: GoogleStorage = mock[GoogleStorage]
  val bucket: GoogleBucket = mock[GoogleBucket]
  val storage: GcsStorage = GcsStorage(underlying)

  s"$GcsStorage" should {

    "implement an async create bucket operation" in {
      //given
      when(underlying.create(any[GoogleBucketInfo])).thenReturn(bucket)

      //when
      val maybeBucket: GcsBucket = storage.createBucket("bucket", Locations.`EUROPE-WEST1`, None).runSyncUnsafe()

      //then
      maybeBucket shouldBe a[GcsBucket]
      verify(underlying, times(1)).create(any[GoogleBucketInfo])
    }

    "implement an async get bucket operation" that {
      "correctly returns some bucket" in {
        //given
        val bucketGetOption: BucketGetOption = mock[BucketGetOption]
        when(underlying.get("bucket", bucketGetOption)).thenReturn(bucket)

        //when
        val maybeBucket: Option[GcsBucket] = storage.getBucket("bucket", bucketGetOption).runSyncUnsafe()

        //then
        maybeBucket.isDefined shouldBe true
        maybeBucket.get shouldBe a[GcsBucket]
        verify(underlying, times(1)).get("bucket", bucketGetOption)
      }

      "safely returns none whenever the underlying response was null" in {
        //given
        val bucketGetOption: BucketGetOption = mock[BucketGetOption]
        when(underlying.get("bucket", bucketGetOption)).thenReturn(null)

        //when
        val maybeBucket: Option[GcsBucket] = storage.getBucket("bucket", bucketGetOption).runSyncUnsafe()

        //then
        maybeBucket.isDefined shouldBe false
        verify(underlying, times(1)).get("bucket", bucketGetOption)
      }
    }

    "implement an async list buckets operation" in {
        // given
        val page = mock[Page[GoogleBucket]]
        val bucketListOption: BucketListOption = mock[BucketListOption]
        when(page.iterateAll()).thenReturn(util.Arrays.asList(bucket, bucket, bucket))
        when(underlying.list(bucketListOption)).thenReturn(page)

        //when
        val maybeBuckets: List[GcsBucket] = storage.listBuckets(bucketListOption).toListL.runSyncUnsafe()

        //then
        maybeBuckets shouldBe a[List[GcsBucket]]
        maybeBuckets.length shouldBe 3
        verify(underlying, times(1)).list(bucketListOption)
    }
  }
}