package io.monix.connect.gcs.streaming

import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

import com.google.cloud.WriteChannel
import com.google.cloud.storage.{BlobInfo, Storage}
import io.monix.connect.gcs.Blob
import monix.eval.Task
import monix.reactive.Observable
import io.monix.connect.{gcs => monix}

trait StorageUploader {

  private def openInputStream(path: Path): Observable[FileInputStream] = {
    Observable.resource {
      Task(new FileInputStream(path.toFile))
    } { is =>
      Task(is.close())
    }
  }

  private def openWriteChannel(blobInfo: BlobInfo)(implicit storage: Storage): Observable[WriteChannel] = {
    Observable.resource {
      Task(storage.writer(blobInfo))
    } { writer =>
      Task(writer.close())
    }
  }

  private def writeToChannel(is: FileInputStream, writer: WriteChannel, chunkSize: Int): Observable[Unit] = {
    Observable.fromInputStream(Task.now(is), chunkSize)
      .takeWhile(_.nonEmpty)
      .mapEval { buffer =>
        Task(writer.write(ByteBuffer.wrap(buffer))).void
      }
  }

  private def upload(blobInfo: BlobInfo, path: Path, chunkSize: Int)(implicit s: Storage): Observable[Unit] = {
    for {
      inputStream  <- openInputStream(path)
      writeChannel <- openWriteChannel(blobInfo)
      _            <- writeToChannel(inputStream, writeChannel, chunkSize)
    } yield ()
  }

  private def getBlobById(blobInfo: BlobInfo)(implicit s: Storage): Task[monix.Blob] = {
    Task(s.get(blobInfo.getBlobId))
      .map(Blob.apply)
  }

  protected def uploadToBucket(blobInfo: BlobInfo, path: Path, chunkSize: Int)(implicit s: Storage): Task[monix.Blob] = {
    if (Files.size(path) <= chunkSize) {
      Task.evalAsync(s.create(blobInfo, Files.readAllBytes(path)))
        .map(Blob.apply)

    } else {
      for {
        _    <- upload(blobInfo, path, chunkSize).completedL
        blob <- getBlobById(blobInfo)
      } yield blob
    }
  }
}
