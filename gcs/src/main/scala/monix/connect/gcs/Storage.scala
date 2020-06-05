package monix.connect.gcs

import java.io.FileInputStream
import java.nio.file.Path

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.Storage._
import com.google.cloud.storage.{StorageOptions, Storage => GoogleStorage}
import monix.connect.gcs.configuration.BucketInfo
import monix.connect.gcs.components.Paging
import monix.connect.gcs.configuration.BucketInfo.{Location, Metadata}
import monix.eval.Task
import monix.reactive.Observable

final class Storage(underlying: GoogleStorage) extends Paging {

  /**
   * Creates a new [[Bucket]] from the give config and options.
   */
  def createBucket(name: String,
                   location: Location,
                   metadata: Option[Metadata],
                   options: BucketTargetOption*)
  : Task[Bucket] = {
    Task(underlying.create(BucketInfo.toJava(name, location, metadata), options: _*))
      .map(Bucket.apply)
  }

  /**
   * Returns the specified bucket or None if it doesn't exist.
   */
  def getBucket(name: String, options: BucketGetOption*): Task[Option[Bucket]] = {
    Task(underlying.get(name, options: _*)).map { optBucket =>
      Option(optBucket).map(Bucket.apply)
    }
  }

  /**
   * Returns an [[Observable]] of all buckets attached to this storage instance.
   */
  def listBuckets(options: BucketListOption *): Observable[Bucket] =
    walk(Task(underlying.list(options: _*))).map(Bucket.apply)
}

object Storage {

  private[gcs] def apply(underlying: GoogleStorage): Storage = {
    new Storage(underlying)
  }

  def create(): Storage = {
    new Storage(StorageOptions.getDefaultInstance.getService)
  }

  def create(projectId: String, credentials: Path): Storage = {
    new Storage(StorageOptions
      .newBuilder()
      .setProjectId(projectId)
      .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credentials.toFile)))
      .build()
      .getService
    )
  }
}