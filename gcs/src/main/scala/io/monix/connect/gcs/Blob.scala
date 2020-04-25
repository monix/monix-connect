package io.monix.connect.gcs

import java.nio.file.Path

import com.google.cloud.storage
import com.google.cloud.storage.Storage
import io.monix.connect.gcs.streaming.StorageDownloader
import monix.eval.Task

final class Blob(blob: storage.Blob) extends StorageDownloader {

  implicit val storage: Storage = blob.getStorage

  def downloadTo(path: Path, chunkSize: Int): Task[Unit] =
    downloadFromBucket(blob.getBlobId, path, chunkSize)

  def exists(): Task[Boolean] =
    Task(blob.exists())

  def reload(): Task[Unit] =
    Task(blob.reload())

  def delete(): Task[Boolean] =
    Task(blob.delete())

}