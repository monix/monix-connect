package io.monix.connect.gcs

import java.nio.file.Path

import com.google.api.gax.paging.Page
import com.google.cloud.{storage, storage => google}
import com.google.cloud.storage.{BlobId, BucketInfo, Storage, StorageOptions}
import io.monix.connect.gcs.configuration.{BlobInfo, BucketConfig}
import io.monix.connect.gcs.streaming.{StorageDownloader, StorageUploader}
import monix.eval.Task
import monix.reactive.Observable

import scala.jdk.CollectionConverters._

final class Bucket(storage: google.Storage, bucket: google.Bucket)
  extends StorageUploader
    with StorageDownloader {

  implicit val s = storage

  /**
   * Retrieves a GCS Blob by name from this Bucket.
   *
   * @param name the name of the blob.
   */
  def getBlob(name: String): Task[Option[Blob]] =
    Task(storage.get(BlobId.of(bucket.getName, name)))
      .map(Option(_))
      .map(Blob.apply)

  /**
   * Uploads a file to this storage bucket. If the file is less than or equal to <code>chunkSize</code> the file is
   * uploaded with a single request, if it is larger, then the file is uploaded in <code>chunkSize</code>
   * batches using a <code>WriteChannel<code>.
   *
   * Example:
   * {{{
   *
   *   val file = Paths.get("/tmp/data.txt")
   *   val bucket: Task[Bucket] = ???
   *
   *   for {
   *    b <- bucket
   *    _ <- b.upload(file)
   *   } yield println("Uploaded File")
   *
   * }}}
   *
   * @param path the path to the file.
   * @param chunkSize the maximum upload chuck size.
   * @param config an optional configuration object for the file.
   */
  def upload(path: Path, chunkSize: Int = 4096, config: Option[BlobInfo] = None): Task[Blob] = {
    val blobId = BlobId.of(bucket.getName, path.getFileName.toString)
    val blobInfo = config
      .map(_.toBlobInfo(blobId))
      .getOrElse(BlobInfo.fromBlobId(blobId))

    uploadToBucket(blobInfo, path, chunkSize)
  }

  /**
   * Downloads a file from this storage bucket, in <code>chunkSize</code> batches using a <code>ReadChannel<code>.
   *
   * Example:
   * {{{
   *
   *   val fileName = "data"
   *   val file = Paths.get("/tmp/data.txt")
   *   val bucket: Task[Bucket] = ???
   *
   *   for {
   *    b <- bucket
   *    _ <- b.download(fileName, file)
   *   } yield println("Downloaded File")
   * }}}
   *
   * @param path the path to the file.
   * @param chunkSize the maximum upload chuck size.
   */
  def download(name: String, path: Path, chunkSize: Int = 4096): Task[Unit] = {
    val blobId = BlobId.of(bucket.getName, name)
    downloadFromBucket(blobId, path, chunkSize)
  }

  /**
   * Returns a [[Observable]] of all blobs in this [[Bucket]].
   *
   * Example:
   * {{{
   *
   *   val config: BucketConfig = BucketConfig("mybucket")
   *   val bucket: Task[Bucket] = Bucket(config)
   *
   *   for {
   *     source <- bucket
   *     blobs  <- source.list().map(b => println(b.name)).completeL
   *   } yield ()
   *
   * }}}
   *
   * @param options options for listing blobs.
   */
  def list(options: Storage.BlobListOption*): Observable[Blob] = {
    def next(page: Page[google.Blob]): Task[(Page[google.Blob], Page[google.Blob])] = {
      if (!page.hasNextPage) {
        Task.now((page, page))
      } else {
        Task(page.getNextPage).map(next => (page, next))
      }
    }

    Observable.fromAsyncStateAction(next)(bucket.list(options: _*))
      .takeWhileInclusive(_.hasNextPage)
      .concatMapIterable(_.iterateAll().asScala.toList)
      .map(Blob.apply)
  }
}

object Bucket {

  // TODO: Abstract into it's own class, handle different authentication methods.
  private def getStorageInstance: Task[Storage] =
    Task(StorageOptions.getDefaultInstance.getService)

  private def getBucketInstance(gcs: Storage, bucketInfo: BucketInfo): Task[google.Bucket] =
    Task(gcs.create(bucketInfo))

  // TODO: Check if bucket exists before creating.
  def apply(config: BucketConfig): Task[Bucket] = {
    for {
      storage    <- getStorageInstance
      bucketInfo <- config.getBucketInfo()
      bucket     <- getBucketInstance(storage, bucketInfo)
    } yield new Bucket(storage, bucket)
  }
}