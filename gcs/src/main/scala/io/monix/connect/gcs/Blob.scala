package io.monix.connect.gcs

import java.io.OutputStream
import java.nio.file.Path

import com.google.cloud.storage
import monix.eval.Task

final class Blob(blob: storage.Blob) {

  def downloadTo(path: Path): Task[Unit] =
    Task(blob.downloadTo(path))

  def downloadTo(outputStream: OutputStream): Task[Unit] =
    Task(blob.downloadTo(outputStream))

  def exists(): Task[Boolean] =
    Task(blob.exists())

  def reload(): Task[Unit] =
    Task(blob.reload())

  def delete(): Task[Boolean] =
    Task(blob.delete())

}