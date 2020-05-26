package monix.connect.gcs.components

import java.nio.ByteBuffer

import cats.effect.Resource
import com.google.cloud.WriteChannel
import com.google.cloud.storage.Storage.BlobWriteOption
import com.google.cloud.storage.{BlobInfo, Storage => GoogleStorage}
import monix.eval.Task

trait StorageUploader {

  private def openWriteChannel(storage: GoogleStorage, blobInfo: BlobInfo, chunkSize: Int, options: BlobWriteOption*): Resource[Task, WriteChannel] = {
    Resource.make {
      Task {
        val writer = storage.writer(blobInfo, options: _*)
        writer.setChunkSize(chunkSize)
        writer
      }
    } { writer =>
      Task(writer.close())
    }
  }

  def upload(storage: GoogleStorage, blobInfo: BlobInfo, chunkSize: Int, options: BlobWriteOption*): Task[StorageConsumer] = {
    openWriteChannel(storage, blobInfo, chunkSize, options: _*).use { channel =>
      Task(StorageConsumer(channel))
    }
  }
}