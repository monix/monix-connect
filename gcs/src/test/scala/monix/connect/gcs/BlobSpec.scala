package monix.connect.gcs

import java.net.URL
import java.util
import java.util.concurrent.TimeUnit

import com.google.cloud.storage.Blob.BlobSourceOption
import com.google.cloud.storage.Storage.{BlobTargetOption, SignUrlOption}
import com.google.cloud.storage.{Acl, BlobId, CopyWriter, Blob => GoogleBlob, Option => _}
import monix.execution.Scheduler.Implicits.global
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.when
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito.{times, verify}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class BlobSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers {

  val underlying: GoogleBlob = mock[GoogleBlob]
  val blob: Blob = Blob(underlying)

  s"$Blob" should {

    "implement an async exists operation" in {
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
        val maybeBlob: Option[Blob] = blob.reload(blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob.isDefined shouldBe true
        maybeBlob.get shouldBe a[Blob]
        verify(underlying, times(1)).reload(blobSourceOption)
      }

      "safely returns none whenever the underlying response was null" in {
        //given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        when(underlying.reload(blobSourceOption)).thenReturn(null)

        //when
        val maybeBlob: Option[Blob] = blob.reload(blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob.isDefined shouldBe false
        verify(underlying, times(1)).reload(blobSourceOption)
      }
    }

    "implement an async update operation that correctly returns some blob" in {
      // given
      val blobTargetOption: BlobTargetOption = mock[BlobTargetOption]
      val googleBlob = mock[GoogleBlob]
      when(underlying.update(blobTargetOption)).thenReturn(googleBlob)

      //when
      val maybeBlob: Blob = blob.update(blobTargetOption).runSyncUnsafe()

      //then
      maybeBlob shouldBe a[Blob]
      verify(underlying, times(1)).update(blobTargetOption)
    }

    "implement an async delete operation" in {
      // given
      val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
      when(underlying.delete(blobSourceOption)).thenReturn(true)

      //when
      val maybeBlob: Boolean = blob.delete(blobSourceOption).runSyncUnsafe()

      //then
      maybeBlob shouldBe true
      verify(underlying, times(1)).delete(blobSourceOption)
    }

    "implement an async copy operation" that {

      "copies this blob to the target blob" in {
        // given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        val blobId = mock[BlobId]
        val copywriter = mock[CopyWriter]
        when(underlying.copyTo(blobId, blobSourceOption)).thenReturn(copywriter)

        //when
        val maybeBlob: Blob = blob.copyTo(blobId, blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob shouldBe a[Blob]
        verify(underlying, times(1)).copyTo(blobId, blobSourceOption)
      }

      "copies this blob to the target bucket" in {
        // given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        val copywriter = mock[CopyWriter]
        when(underlying.copyTo("bucket2", blobSourceOption)).thenReturn(copywriter)

        //when
        val maybeBlob: Blob = blob.copyTo("bucket2", blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob shouldBe a[Blob]
        verify(underlying, times(1)).copyTo("bucket2", blobSourceOption)
      }

      "copies this blob to the target blob in the target bucket" in {
        // given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        val copywriter = mock[CopyWriter]
        when(underlying.copyTo("bucket2", "blob2", blobSourceOption)).thenReturn(copywriter)

        //when
        val maybeBlob: Blob = blob.copyTo("bucket2", "blob2", blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob shouldBe a[Blob]
        verify(underlying, times(1)).copyTo("bucket2", "blob2", blobSourceOption)
      }
    }

    "implement an async sign url operation that correctly returns some url" in {
      // given
      val signUrlOption: SignUrlOption = mock[SignUrlOption]
      val url = mock[URL]
      when(underlying.signUrl(2, TimeUnit.MINUTES, signUrlOption)).thenReturn(url)

      //when
      val maybeUrl: URL = blob.signUrl(2.minutes, signUrlOption).runSyncUnsafe()

      maybeUrl shouldBe a[URL]
      verify(underlying, times(1)).signUrl(2, TimeUnit.MINUTES, signUrlOption)
    }

    "implement an async create acl operation that correctly returns some acl" in {
      // given
      val acl = mock[Acl]
      when(underlying.createAcl(acl)).thenReturn(acl)

      //when
      val maybeAcl: Acl = blob.createAcl(acl).runSyncUnsafe()

      //then
      maybeAcl shouldBe a[Acl]
      verify(underlying, times(1)).createAcl(acl)
    }

    "implement an async get acl operation" that {

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
        val aclEntity = mock[Acl.Entity]
        val acl = mock[Acl]
        when(underlying.getAcl(aclEntity)).thenReturn(acl)

        //when
        val maybeAcl: Option[Acl] = blob.getAcl(aclEntity).runSyncUnsafe()

        //then
        maybeAcl.isDefined shouldBe true
        maybeAcl.get shouldBe a[Acl]
        verify(underlying, times(1)).getAcl(aclEntity)
      }
    }

    "implement an async update operation that correctly returns some acl" in {
      // given
      val acl = mock[Acl]
      when(underlying.updateAcl(acl)).thenReturn(acl)

      //when
      val maybeAcl: Acl = blob.updateAcl(acl).runSyncUnsafe()

      //then
      maybeAcl shouldBe a[Acl]
      verify(underlying, times(1)).updateAcl(acl)
    }

    "implement an async delete operation that deletes the specified acl" in {
      // given
      val acl = mock[Acl.Entity]
      when(underlying.deleteAcl(acl)).thenReturn(true)

      //when
      val result: Boolean = blob.deleteAcl(acl).runSyncUnsafe()

      //then
      result shouldBe true
      verify(underlying, times(1)).deleteAcl(acl)
    }

    "implement an async list acl operation that correctly returns zero or more acls" in {
      // given
      val acl = mock[Acl]
      val acls = util.Arrays.asList(acl, acl, acl)
      when(underlying.listAcls()).thenReturn(acls)

      //when
      val result: List[Acl] = blob.listAcls().toListL.runSyncUnsafe()

      //then
      result shouldBe List(acl, acl, acl)
      verify(underlying, times(1)).listAcls()
    }
  }
}
