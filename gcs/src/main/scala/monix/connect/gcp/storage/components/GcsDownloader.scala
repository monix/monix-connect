package monix.connect.gcp.storage.components

import java.nio.channels.Channels

import com.google.cloud.ReadChannel
import com.google.cloud.storage.{BlobId, Storage}
import monix.eval.Task
import monix.reactive.Observable

/** An internal class that provides the necessary implementations for downloading
  * blobs from any GCS bucket in form of a byte array [[Observable]].
  */
private[storage] trait GcsDownloader {

  /**
    * Provides a safe way to open (acquire) and close (release) a [[ReadChannel]] using resource signature.
    * @param storage underlying [[Storage]] instance.
    * @param blobId the source blob id to download from.
    * @param chunkSize conforms the size in bytes of each future read element.
    * @return an [[Observable]] that exposes a [[ReadChannel]] to read from.
    */
  private def openReadChannel(storage: Storage, blobId: BlobId, chunkSize: Int): Observable[ReadChannel] =
    Observable.resource {
      Task {
        val reader = storage.reader(blobId.getBucket, blobId.getName)
        reader.setChunkSize(chunkSize)
        reader
      }
    } { reader =>
      Task(reader.close())
    }

  /**
    * Downloads the content from a Blob in form an array byte [[Observable]] of the specified chunksize.
    * @param storage underlying [[Storage]] instance.
    * @param blobId the source blob id to download from.
    * @param chunkSize conforms the size in bytes of each future read element.
    * @return an array bytes [[Observable]].
    */
  protected def download(storage: Storage, blobId: BlobId, chunkSize: Int): Observable[Array[Byte]] =
    openReadChannel(storage, blobId, chunkSize).flatMap { channel =>
      Observable.fromInputStreamUnsafe(Channels.newInputStream(channel), chunkSize)
    }.takeWhile(_.nonEmpty)

}