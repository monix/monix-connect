package monix.connect.gcp.storage.components

import com.google.cloud.ReadChannel
import com.google.cloud.storage.{ BlobId, Storage, Blob => GoogleBlob, Option => _}
import monix.execution.Scheduler.Implicits.global
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{doNothing, when}
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito.{times, verify}
import org.mockito.ArgumentMatchers.any
import org.scalatest.wordspec.AnyWordSpecLike


class GcsDownloaderSpec extends AnyWordSpecLike with IdiomaticMockito with Matchers {

  val underlying: GoogleBlob = mock[GoogleBlob]
  val mockStorage: Storage = mock[Storage]
  val readChannel: ReadChannel = mock[ReadChannel]

  s"GcsDownloader" should {

    "download a blob" in new GcsDownloader {
      //given
      val bucket = "sampleBucket"
      val blobName = "sampleBlob"
      val chunkSize = 100
      when(mockStorage.reader(bucket, blobName)).thenReturn(readChannel)
      doNothing.when(readChannel).setChunkSize(chunkSize)

      //when
      download(mockStorage, BlobId.of(bucket, blobName), chunkSize).toListL.runSyncUnsafe().flatten

      //then
      verify(mockStorage, times(1)).reader(bucket, blobName)
      verify(readChannel, times(1)).read(any())
      verify(readChannel, times(1)).close()
    }

  }
}
