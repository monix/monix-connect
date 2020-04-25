package io.monix.connect.gcs.streaming

import java.io.FileOutputStream
import java.nio.channels.Channels
import java.nio.file.Path

import com.google.cloud.ReadChannel
import com.google.cloud.storage.{BlobId, Storage}
import monix.eval.Task
import monix.reactive.Observable

trait StorageDownloader {

  private def openOutputStream(path: Path): Observable[FileOutputStream] = {
    Observable.resource {
      Task(new FileOutputStream(path.toFile))
    } { os =>
      Task(os.close())
    }
  }

  private def openReadChannel(blobId: BlobId)(implicit storage: Storage): Observable[ReadChannel] = {
    Observable.resource {
      Task(storage.reader(blobId))
    } { reader =>
      Task(reader.close())
    }
  }

  private def readFromChannel(reader: ReadChannel, os: FileOutputStream, chunkSize: Int): Observable[Unit] = {
    Observable.fromInputStream(Task.now(Channels.newInputStream(reader)), chunkSize)
      .takeWhile(_.nonEmpty)
      .mapEval { buffer =>
        Task(os.write(buffer)).void
      }
  }

  protected def downloadFromBucket(blobId: BlobId, path: Path, chunkSize: Int)
                                  (implicit storage: Storage): Task[Unit] = {
    (for {
      os <- openOutputStream(path)
      rd <- openReadChannel(blobId)
      _  <- readFromChannel(rd, os, chunkSize)
    } yield ()).completedL
  }
}
