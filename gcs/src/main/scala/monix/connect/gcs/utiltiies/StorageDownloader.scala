package monix.connect.gcs.utiltiies

import java.io.FileOutputStream
import java.nio.channels.Channels
import java.nio.file.Path

import cats.effect.Resource
import com.google.cloud.ReadChannel
import com.google.cloud.storage.{BlobId, Storage => GoogleStorage}
import monix.eval.Task
import monix.reactive.Observable

trait StorageDownloader {

  private def openReadChannel(storage: GoogleStorage, bucket: String, blobId: BlobId): Resource[Task, ReadChannel] = {
    Resource.make {
      Task(storage.reader(bucket, blobId.getName))
    } { reader =>
      Task(reader.close())
    }
  }

  private def openOutputStream(path: Path): Resource[Task, FileOutputStream] = {
    Resource.make {
      Task(new FileOutputStream(path.toFile))
    } { os =>
      Task(os.close())
    }
  }

  private def download(reader: ReadChannel, os: FileOutputStream, chunkSize: Int): Task[Unit] = {
    Observable
      .fromInputStreamUnsafe(Channels.newInputStream(reader), chunkSize)
      .takeWhile(_.nonEmpty)
      .mapEval(bytes => Task(os.write(bytes)))
      .completedL
  }

  protected def downloadFromBucket(storage: GoogleStorage, bucket: String, blobId: BlobId, path: Path, chunkSize: Int): Task[Unit] = {
    (for {
      rc <- openReadChannel(storage, bucket, blobId)
      os <- openOutputStream(path)
    } yield (os, rc)).use { case (output, channel) =>
      download(channel, output, chunkSize)
    }
  }
}
