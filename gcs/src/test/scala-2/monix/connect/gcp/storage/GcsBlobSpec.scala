/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
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

import com.google.cloud.storage.Blob.BlobSourceOption
import com.google.cloud.storage.Storage.BlobTargetOption
import com.google.cloud.storage.{Acl, CopyWriter, Blob => GoogleBlob, Option => _}
import monix.execution.Scheduler.Implicits.global
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.when
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito.{times, verify}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

import scala.jdk.CollectionConverters._

class GcsBlobSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers with GscFixture with BeforeAndAfterEach {

  val underlying: GoogleBlob = mock[GoogleBlob]
  val blob: GcsBlob = GcsBlob(underlying)

  override def beforeEach(): Unit = {
    super.beforeEach()
    reset(underlying)
  }

  s"$GcsBlob" should {

    "implement an exists operation" in {
      //given
      val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
      when(underlying.exists(blobSourceOption)).thenAnswer(true)

      //when
      val result: Boolean = blob.exists(blobSourceOption).runSyncUnsafe()

      //then
      result shouldBe true
      verify(underlying, times(1)).exists(blobSourceOption)
    }

    "implement reload method" that {

      "correctly returns some blob" in {
        //given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        val googleBlob = mock[GoogleBlob]
        when(underlying.reload(blobSourceOption)).thenReturn(googleBlob)

        //when
        val maybeBlob: Option[GcsBlob] = blob.reload(blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob.isDefined shouldBe true
        maybeBlob.get shouldBe a[GcsBlob]
        verify(underlying, times(1)).reload(blobSourceOption)
      }

      "safely returns none whenever the underlying response was null" in {
        //given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        when(underlying.reload(blobSourceOption)).thenReturn(null)

        //when
        val maybeBlob: Option[GcsBlob] = blob.reload(blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob.isDefined shouldBe false
        verify(underlying, times(1)).reload(blobSourceOption)
      }
    }

    "implement an update operation that correctly returns some blob" in {
      // given
      val blobTargetOption: BlobTargetOption = mock[BlobTargetOption]
      val googleBlob = mock[GoogleBlob]
      when(underlying.update(blobTargetOption)).thenReturn(googleBlob)

      //when
      val maybeBlob: GcsBlob = blob.update(blobTargetOption).runSyncUnsafe()

      //then
      maybeBlob shouldBe a[GcsBlob]
      verify(underlying, times(1)).update(blobTargetOption)
    }

    "implement an delete operation" in {
      // given
      val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
      when(underlying.delete(blobSourceOption)).thenReturn(true)

      //when
      val maybeBlob: Boolean = blob.delete(blobSourceOption).runSyncUnsafe()

      //then
      maybeBlob shouldBe true
      verify(underlying, times(1)).delete(blobSourceOption)
    }

    "implement an copy operation" that {

      "copies this blob to the target blob id" in {
        // given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        val blobId = genBlobId.sample.get
        val copywriter = mock[CopyWriter]
        when(underlying.copyTo(blobId, blobSourceOption)).thenReturn(copywriter)

        //when
        val maybeBlob: GcsBlob = blob.copyTo(blobId, blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob shouldBe a[GcsBlob]
        verify(underlying, times(1)).copyTo(blobId, blobSourceOption)
      }

      "copies this blob to the target bucket" in {
        // given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        val copywriter = mock[CopyWriter]
        val bucketName = genNonEmtyStr.sample.get
        when(underlying.copyTo(bucketName, blobSourceOption)).thenReturn(copywriter)

        //when
        val maybeBlob: GcsBlob = blob.copyTo(bucketName, blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob shouldBe a[GcsBlob]
        verify(underlying, times(1)).copyTo(bucketName, blobSourceOption)
      }

      "copies this blob to the target blob in the target bucket" in {
        // given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        val copywriter = mock[CopyWriter]
        val bucketName = genNonEmtyStr.sample.get
        val blobName = genNonEmtyStr.sample.get
        when(underlying.copyTo(bucketName, blobName, blobSourceOption)).thenReturn(copywriter)

        //when
        val maybeBlob: GcsBlob = blob.copyTo(bucketName, blobName, blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob shouldBe a[GcsBlob]
        verify(underlying, times(1)).copyTo(bucketName, blobName, blobSourceOption)
      }
    }

    // "implement an async sign url operation that correctly returns some url" in {
    //   // given
    //   val signUrlOption: SignUrlOption = mock[SignUrlOption]
    //   val url = new URL("TCP")
    //   when(underlying.signUrl(2, TimeUnit.MINUTES, signUrlOption)).thenReturn(url)
    //
    //   //when
    //   val maybeUrl: URL = blob.signUrl(2.minutes, signUrlOption).runSyncUnsafe()
    //
    //   maybeUrl shouldBe a[URL]
    //   verify(underlying, times(1)).signUrl(2, TimeUnit.MINUTES, signUrlOption)
    // }

    "implement a create acl operation that correctly returns some acl" in {
      // given
      val acl = genAcl.sample.get
      when(underlying.createAcl(acl)).thenReturn(acl)

      //when
      val maybeAcl: Acl = blob.createAcl(acl).runSyncUnsafe()

      //then
      maybeAcl shouldBe a[Acl]
      verify(underlying, times(1)).createAcl(acl)
    }

    "implement a get acl operation" that {

      "that safely returns none whenever the underlying response was null" in {
        // given
        val acl = mock[Acl.Entity]
        when(underlying.getAcl(acl)).thenReturn(null)

        //when
        val maybeAcl: Option[Acl] = blob.getAcl(acl).runSyncUnsafe()

        //then
        maybeAcl.isDefined shouldBe false
        verify(underlying, times(1)).getAcl(acl)
      }

      "that correctly returns some acl" in {
        // given
        val acl = genAcl.sample.get
        when(underlying.getAcl(acl.getEntity)).thenReturn(acl)

        //when
        val maybeAcl: Option[Acl] = blob.getAcl(acl.getEntity).runSyncUnsafe()

        //then
        maybeAcl.isDefined shouldBe true
        maybeAcl.get shouldBe a[Acl]
        verify(underlying, times(1)).getAcl(acl.getEntity)
      }
    }

    "implement a update operation that correctly returns some acl" in {
      // given
      val acl = genAcl.sample.get
      when(underlying.updateAcl(acl)).thenReturn(acl)

      //when
      val maybeAcl: Acl = blob.updateAcl(acl).runSyncUnsafe()

      //then
      maybeAcl shouldBe a[Acl]
      verify(underlying, times(1)).updateAcl(acl)
    }

    "implement a delete operation that deletes the specified acl" in {
      // given
      val acl = mock[Acl.Entity]
      when(underlying.deleteAcl(acl)).thenReturn(true)

      //when
      val result: Boolean = blob.deleteAcl(acl).runSyncUnsafe()

      //then
      result shouldBe true
      verify(underlying, times(1)).deleteAcl(acl)
    }

    "implement a list acl operation that correctly returns zero or more acls" in {
      // given
      val acls = Gen.nonEmptyListOf(genAcl).sample.get
      when(underlying.listAcls()).thenReturn(acls.asJava)

      //when
      val result: List[Acl] = blob.listAcls().toListL.runSyncUnsafe()

      //then
      result shouldBe acls
      verify(underlying, times(1)).listAcls()
    }
  }
}
