/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.gcp.storage

import java.util

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Storage.{BlobListOption, BucketGetOption, BucketListOption}
import com.google.cloud.storage.{
  Blob,
  Bucket => GoogleBucket,
  BucketInfo => GoogleBucketInfo,
  Storage => GoogleStorage,
  Option => _
}
import monix.connect.gcp.storage.configuration.GcsBucketInfo.Locations
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito.{times, verify}
import org.mockito.MockitoSugar.when
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class GcsStorageSpec
  extends AnyWordSpecLike with IdiomaticMockito with Matchers with ArgumentMatchersSugar with GscFixture
  with BeforeAndAfterEach {
  val underlying: GoogleStorage = mock[GoogleStorage]
  val bucket: GoogleBucket = mock[GoogleBucket]
  val storage: GcsStorage = GcsStorage(underlying)

  override def beforeEach: Unit = {
    super.beforeEach()
    reset(underlying)
  }

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

    "implement a list buckets operation" in {
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

    "implement a list blobs operation" in {
      // given
      val page = mock[Page[Blob]]
      val blob = mock[Blob]
      val bucketName = genNonEmtyStr.sample.get
      val blobListOption: BlobListOption = mock[BlobListOption]
      when(page.iterateAll()).thenReturn(util.Arrays.asList(blob, blob, blob))
      when(underlying.list(bucketName, blobListOption)).thenReturn(page)

      //when
      val maybeBuckets: List[GcsBlob] = storage.listBlobs(bucketName, blobListOption).toListL.runSyncUnsafe()

      //then
      maybeBuckets shouldBe a[List[GcsBucket]]
      maybeBuckets.length shouldBe 3
      verify(underlying, times(1)).list(bucketName, blobListOption)
    }

  }
}
