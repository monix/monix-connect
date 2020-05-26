package monix.connect.gcs

import com.google.cloud.storage.Blob.BlobSourceOption
import org.mockito.IdiomaticMockito
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar.when
import com.google.cloud.storage.{Blob => GoogleBlob, Option => _}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.wordspec.AnyWordSpecLike

class BlobSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers {

  val underlying: GoogleBlob = mock[GoogleBlob]

  val blob = Blob(underlying)

  s"${Blob}" should {

    "implement an async exists operation" in {
      //given
      val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
      when(underlying.exists(blobSourceOption)).thenAnswer(true)

      //when
      val t: Task[Boolean] = blob.exists(blobSourceOption)

      //then
      val exists = t.runSyncUnsafe()
      exists shouldBe true
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
      }

      "safely returns none whenever the underlying response was null" in {
        //given
        val blobSourceOption: BlobSourceOption = mock[BlobSourceOption]
        when(underlying.reload(blobSourceOption)).thenReturn(null)

        //when
        val maybeBlob: Option[Blob] = blob.reload(blobSourceOption).runSyncUnsafe()

        //then
        maybeBlob.isDefined shouldBe false
      }
    }

  }
}
