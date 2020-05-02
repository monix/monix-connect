package monix.connect.gcs.utiltiies

import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.file.Path

import cats.effect.Resource
import com.google.cloud.WriteChannel
import com.google.cloud.storage.Storage.BlobWriteOption
import com.google.cloud.storage.{BlobInfo, Storage => GoogleStorage}
import monix.eval.Task
import monix.reactive.Observable

trait StorageUploader {

  private def openInputStream(path: Path): Resource[Task, FileInputStream] = {
    Resource.make {
      Task(new FileInputStream(path.toFile))
    } { fis =>
      Task(fis.close())
    }
  }

  private def openWriteChannel(storage: GoogleStorage, blobInfo: BlobInfo, options: BlobWriteOption*): Resource[Task, WriteChannel] = {
    Resource.make {
      Task(storage.writer(blobInfo, options: _*))
    } { writer =>
      Task(writer.close())
    }
  }

  private def upload(is: FileInputStream, writer: WriteChannel, chunkSize: Int): Task[Unit] = {
    Observable
      .fromInputStreamUnsafe(is, chunkSize)
      .takeWhile(_.nonEmpty)
      .mapEval(bytes => Task(writer.write(ByteBuffer.wrap(bytes))))
      .completedL
  }

  def uploadToBucket(storage: GoogleStorage, blobInfo: BlobInfo, path: Path, chunkSize: Int, options: BlobWriteOption*): Task[Unit] = {
    (for {
      is <- openInputStream(path)
      wc <- openWriteChannel(storage, blobInfo, options: _*)
    } yield (is, wc)).use { case (input, channel) =>
      upload(input, channel, chunkSize)
    }
  }
}
