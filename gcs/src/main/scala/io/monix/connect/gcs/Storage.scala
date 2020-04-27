package io.monix.connect.gcs

import com.google.cloud.storage.Storage._
import com.google.cloud.storage.{StorageOptions, Storage => GoogleStorage}
import io.monix.connect.gcs.configuration.BucketConfig
import io.monix.connect.gcs.utiltiies.Paging
import monix.eval.Task
import monix.reactive.Observable

final class Storage(underlying: GoogleStorage) extends Paging {
  def createBucket(config: BucketConfig, options: BucketTargetOption*): Task[Bucket] =
    Task(underlying.create(config.getBucketInfo, options: _*))
      .map(Bucket.apply)

  def getBucket(name: String, options: BucketGetOption*): Task[Option[Bucket]] = {
    Task(underlying.get(name, options: _*)).map { optBucket =>
      Option(optBucket).map(Bucket.apply)
    }
  }

  def listBuckets(options: BucketListOption *): Observable[Bucket] =
    walk(Task(underlying.list(options: _*)))
      .map(Bucket.apply)
}

object Storage {
  def create(): Task[Storage] = {
    Task(StorageOptions.getDefaultInstance.getService).map(new Storage(_))
  }
}