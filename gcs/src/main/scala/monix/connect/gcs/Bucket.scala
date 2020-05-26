package monix.connect.gcs

import java.nio.file.Path

import cats.data.NonEmptyList
import com.google.cloud.storage.Bucket.BucketSourceOption
import com.google.cloud.storage.Storage.{BlobGetOption, BlobListOption, BlobWriteOption, BucketTargetOption}
import com.google.cloud.storage.{Acl, BlobId, Bucket => GoogleBucket}
import monix.connect.gcs.configuration.BlobInfo
import monix.connect.gcs.components.{FileIO, Paging, StorageDownloader, StorageUploader}
import monix.eval.Task
import monix.reactive.Observable
import monix.connect.gcs.components.StorageConsumer

import scala.collection.JavaConverters._

/**
 * This class wraps the [[com.google.cloud.storage.Bucket]] class, providing an idiomatic scala API
 * handling null values with [[Option]] where applicable, as well as wrapping all side-effectful calls
 * in [[monix.eval.Task]] or [[monix.reactive.Observable]].
 *
 * Example:
 * {{{
 *   import monix.reactive.Observable
 *   import monix.connect.gcs.{Storage, Bucket}
 *
 *   val config = BucketConfig(
 *      name = "mybucket"
 *   )
 *
 *   val storage: Task[Storage] = Storage.create().memoize
 *   val bucket: Task[Bucket] = storage.flatMap(_.createBucket(config)).memoize
 *
 *   (for {
 *      bucket <- Observable.fromTask(bucket)
 *      blobs  <- bucket.list()
 *   } yield println(blob.name)).completeL
 * }}}
 */
final class Bucket private(underlying: GoogleBucket)
  extends StorageUploader
    with StorageDownloader
    with FileIO
    with Paging {

  /**
   * Downloads a Blob from GCS, returning an Observable containing the bytes in chunks of length chunkSize.
   *
   * Example:
   * {{{
   *   import java.nio.charset.StandardCharsets
   *
   *   import monix.reactive.Observable
   *   import monix.connect.gcs.{Storage, Bucket}
   *
   *   val config = BucketConfig(
   *      name = "mybucket"
   *   )
   *
   *   val storage: Task[Storage] = Storage.create().memoize
   *   val bucket: Task[Bucket] = storage.flatMap(_.createBucket(config)).memoize
   *
   *   // Download the blob contents to a String and print it to the console.
   *   for {
   *      bucket <- bucket
   *      bytes  <- b.download("blob1").foldLeftL(Array.emptyByteArray)(_ ++ _)
   *   } yield println(new String(bytes, StandardCharsets.UTF_8))
   * }}}
   *
   *
   */
  def download(name: String, chunkSize: Int = 4096): Observable[Array[Byte]] = {
    download(underlying.getStorage, underlying.getName, BlobId.of(underlying.getName, name), chunkSize)
  }

  /**
   * Allows downloading a Blob from GCS directly to the specified file.
   *
   * Example:
   * {{{
   *   import java.nio.file.Paths
   *
   *   import monix.reactive.Observable
   *   import monix.connect.gcs.{Storage, Bucket}
   *
   *   val config = BucketConfig(
   *      name = "mybucket"
   *   )
   *
   *   val storage: Task[Storage] = Storage.create().memoize
   *   val bucket: Task[Bucket] = storage.flatMap(_.createBucket(config)).memoize
   *
   *   for {
   *      b <- bucket
   *      _ <- bucket.downloadToFile("blob1", Paths.get("file.txt"))
   *   } yield println("File downloaded Successfully")
   * }}}
   */
  def downloadToFile(name: String, path: Path, chunkSize: Int = 4096): Task[Unit] = {
    val blobId = BlobId.of(underlying.getName, name)
    (for {
      bos   <- openFileOutputStream(path)
      bytes <- download(underlying.getStorage, underlying.getName, blobId, chunkSize)
    } yield bos.write(bytes)).completedL
  }


  /**
   * Returns a new Consumer that will upload bytes to the specified target Blob.
   *
   * Example:
   * {{{
   *   import java.nio.charset.StandardCharsets
   *
   *   import monix.reactive.Observable
   *   import monix.connect.gcs.{Storage, Bucket}
   *
   *   val config = BucketConfig(
   *      name = "mybucket"
   *   )
   *
   *   val storage: Task[Storage] = Storage.create().memoize
   *   val bucket: Task[Bucket] = storage.flatMap(_.createBucket(config)).memoize
   *
   *   val data = "mydata".getBytes(StandardCharsets.UTF_8)
   *
   *   for {
   *     b <- bucket
   *     c <- b.upload("mydata")
   *     _ <- Observable.fromIterable(data).consumeWith(c)
   *   } println("Uploaded Data Successfully")
   * }}}
   */
  def upload(name: String,
             metadata: Option[BlobInfo.Metadata] = None,
             chunkSize: Int = 4096,
             options: List[BlobWriteOption] = List.empty[BlobWriteOption]
  ): Task[StorageConsumer] = {
    val blobInfo = BlobInfo.toJava(underlying.getName, name, metadata)
    upload(underlying.getStorage, blobInfo, chunkSize, options: _*)
  }

  /**
   * Uploads the provided file to the specified target Blob.
   *
   * Example:
   * {{{
   *   import java.nio.file.Paths
   *
   *   import monix.reactive.Observable
   *   import monix.connect.gcs.{Storage, Bucket}
   *
   *   val config = BucketConfig(
   *      name = "mybucket"
   *   )
   *
   *   val storage: Task[Storage] = Storage.create().memoize
   *   val bucket: Task[Bucket] = storage.flatMap(_.createBucket(config)).memoize
   *
   *   for {
   *      b <- bucket
   *      _ <- bucket.uploadFromFile("blob1", Paths.get("file.txt"))
   *   } yield println("File Uploaded Successfully")
   * }}}
   */
  def uploadFromFile(name: String,
                     path: Path,
                     metadata: Option[BlobInfo.Metadata] = None,
                     chunkSize: Int = 4096,
                     options: List[BlobWriteOption] = List.empty[BlobWriteOption]
  ): Task[Unit] = {
    val blobInfo = BlobInfo.toJava(underlying.getName, name, metadata)
    upload(underlying.getStorage, blobInfo, chunkSize, options: _*).flatMap { consumer =>
      openFileInputStream(path).flatMap { fis =>
        Observable.fromInputStreamUnsafe(fis).takeWhile(_.nonEmpty)
      }.consumeWith(consumer)
    }
  }

  /**
   * Checks if this bucket exists.
   */
  def exists(options: BucketSourceOption*): Task[Boolean] =
    Task(underlying.exists(options: _*))

  /**
   * Reloads and refreshes this buckets data, returning a new Bucket instance.
   */
  def reload(options: BucketSourceOption*): Task[Option[Bucket]] =
    Task(underlying.reload(options: _*)).map { optBucket =>
      Option(optBucket).map(Bucket.apply)
    }

  /**
   * Updates this bucket with the provided options, returning the newly updated
   * Bucket instance.
   *
   * By default no checks are made on the metadata generation of the current bucket. If
   * you want to update the information only if the current bucket metadata are at their latest
   * version use the [[BucketTargetOption.metagenerationMatch]] option.
   */
  def update(options: BucketTargetOption*): Task[Bucket] =
    Task(underlying.update(options: _*))
      .map(Bucket.apply)

  /**
   * Deletes this bucket.
   */
  def delete(options: BucketSourceOption*): Task[Boolean] =
    Task(underlying.delete(options: _*))

  /**
   * Returns the requested blob in this bucket or None if it isn't found.
   */
  def getBlob(name: String, options: BlobGetOption*): Task[Option[Blob]] = {
    Task(underlying.get(name, options: _*)).map { optBlob =>
      Option(optBlob).map(Blob.apply)
    }
  }

  /**
   * Returns an [[Observable]] of the requested blobs, if one doesn't exist null is
   * returned and filtered out of the result set.
   */
  def getBlobs(names: NonEmptyList[String]): Observable[Blob] = Observable.suspend {
    Observable
      .fromIterable(underlying.get(names.toList.asJava).asScala)
      .filter(_ != null)
      .map(Blob.apply)
  }

  /**
   * Returns a [[Observable]] of all blobs in this [[Bucket]].
   */
  def listBlobs(options: BlobListOption*): Observable[Blob] = {
    walk(Task(underlying.list(options: _*))).map(Blob.apply)
  }

  /**
   * Creates a new ACL entry on this bucket.
   */
  def createAcl(acl: Acl): Task[Acl] =
    Task(underlying.createAcl(acl))

  /**
   * Returns the ACL entry for the specified entity on this bucket or None if not found.
   */
  def getAcl(acl: Acl.Entity): Task[Option[Acl]] =
    Task(underlying.getAcl(acl)).map(Option(_))

  /**
   * Updates an ACL entry on this bucket.
   */
  def updateAcl(acl: Acl): Task[Acl] =
    Task(underlying.updateAcl(acl))

  /**
   * Deletes the ACL entry for the specified entity on this bucket.
   */
  def deleteAcl(acl: Acl.Entity): Task[Boolean] =
    Task(underlying.deleteAcl(acl))

  /**
   * Returns a [[Observable]] of all the ACL Entries for this [[Bucket]].
   */
  def listAcls(): Observable[Acl] = {
    Observable.suspend {
      Observable.fromIterable(underlying.listAcls().asScala)
    }
  }

  /**
   * Creates a new default blob ACL entry on this bucket. Default ACLs are applied to a new blob within the bucket
   * when no ACL was provided for that blob.
   */
  def createDefaultAcl(acl: Acl): Task[Acl] =
    Task(underlying.createDefaultAcl(acl))

  /**
   * Returns the default object ACL entry for the specified entity on this bucket or None if
   * not found.
   */
  def getDefaultAcl(acl: Acl.Entity): Task[Option[Acl]] =
    Task(underlying.getDefaultAcl(acl)).map(Option(_))

  /**
   * Updates a default blob ACL entry on this bucket.
   */
  def updateDefaultAcl(acl: Acl): Task[Acl] =
    Task(underlying.updateDefaultAcl(acl))

  /**
   * Deletes the default object ACL entry for the specified entity on this bucket.
   */
  def deleteDefaultAcl(acl: Acl.Entity): Task[Boolean] =
    Task(underlying.deleteDefaultAcl(acl))

  /**
   * Returns a [[Observable]] of all the default Blob ACL Entries for this [[Bucket]].
   */
  def listDefaultAcls(): Observable[Acl] = {
    Observable.suspend {
      Observable.fromIterable(underlying.listDefaultAcls().asScala)
    }
  }

  /**
   * Locks bucket retention policy. Requires a local metageneration value in the request.
   */
  def lockRetentionPolicy(options: BucketTargetOption*): Task[Bucket] =
    Task(underlying.lockRetentionPolicy(options: _*)).map(Bucket.apply)
}

object Bucket {
  private[gcs] def apply(bucket: GoogleBucket): Bucket = {
    new Bucket(bucket)
  }
}