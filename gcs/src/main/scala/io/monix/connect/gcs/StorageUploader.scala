package io.monix.connect.gcs

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.nio.file.Files

import cats.implicits._
import com.google.cloud.storage.{Blob, Storage}
import com.google.cloud.{WriteChannel, storage => gcs}
import monix.eval.Task
import monix.reactive.Observable

trait StorageUploader {

  private val `1MB`= 1000000L

  private def openInputStream(file: File): Observable[FileInputStream] = {
    Observable.resource {
      Task(new FileInputStream(file))
    } { is =>
      Task(is.close())
    }
  }

  private def openWriteChannel(blobInfo: gcs.BlobInfo)(implicit storage: Storage): Observable[WriteChannel] = {
    Observable.resource {
      Task(storage.writer(blobInfo))
    } { writer =>
      Task(writer.close())
    }
  }

  // TODO: Resumeable Uploads
  private def writeToChannel(is: FileInputStream, writer: WriteChannel, writeBufferSize: Int): Observable[Int] = {
    Observable.fromInputStream(Task.now(is), writeBufferSize)
      .takeWhile(_.nonEmpty)
      .mapEval(buffer => Task(writer.write(ByteBuffer.wrap(buffer))))
      .fold
  }

  /**
   * If the file size is below 1MB we can upload the file in a single request, otherwise, we upload the file in
   * chunks of size $writeBufferSize.
   */
  protected def uploadToBucket(blobInfo: gcs.BlobInfo, file: File, writeBufferSize: Int)(implicit storage: Storage): Task[Blob] = {
    if (Files.size(file.toPath) <= `1MB`) {
      Task.evalAsync(storage.create(blobInfo, Files.readAllBytes(file.toPath)))
    } else {
      (for {
        inputStream  <- openInputStream(file)
        writeChannel <- openWriteChannel(blobInfo)
        _            <- writeToChannel(inputStream, writeChannel, writeBufferSize)
        blob         <- Observable.fromTask(Task(storage.get(blobInfo.getBlobId)))
      } yield blob).firstL
    }
  }
}
