package monix.connect.gcs

import java.net.URL
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import com.google.cloud.storage.Blob.BlobSourceOption
import com.google.cloud.storage.Storage.{BlobTargetOption, SignUrlOption}
import com.google.cloud.storage.{Acl, BlobId, Blob => GoogleBlob, Option => _}
import com.google.cloud.{storage => google}
import monix.connect.gcs.configuration.BlobInfo
import monix.connect.gcs.utiltiies.{FileIO, StorageDownloader}
import monix.eval.Task
import monix.reactive.Observable

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
 * This class wraps the [[com.google.cloud.storage.Blob]] class, providing an idiomatic scala API
 * handling null values with [[Option]] where applicable, as well as wrapping all side-effectful calls
 * in [[monix.eval.Task]] or [[monix.reactive.Observable]].
 *
 * @define copyToNote Forcing an Async Boundary, this function potentially spins until the copy is done. If the src and
 *                    dst are in the same location and share the same storage class the request is done in one RPC call,
 *                    otherwise multiple calls are issued.
 */
final class Blob(underlying: GoogleBlob)
  extends StorageDownloader
    with FileIO {

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
   * Checks if this blob exists.
   */
  def exists(options: BlobSourceOption*): Task[Boolean] =
    Task(underlying.exists(options: _*))

  /**
   * Fetches current blob's latest information. Returns None if the blob does not exist.
   */
  def reload(options: BlobSourceOption*): Task[Option[Blob]] =
    Task(underlying.reload(options: _*)).map { optBlob =>
      Option(optBlob).map(Blob.apply)
    }

  /**
   * Updates the blob's information. The Blob's name cannot be changed by this method. If you
   * want to rename the blob or move it to a different bucket use the [[copyTo]] and [[delete]] operations.
   */
  def update(options: BlobTargetOption*): Task[Blob] = {
    Task(underlying.update(options: _*))
      .map(Blob.apply)
  }

  /**
   * Updates the blob's information. Bucket or blob's name cannot be changed by this method. If you
   * want to rename the blob or move it to a different bucket use the [[copyTo]] and [[delete]] operations.
   */
  def update(metadata: BlobInfo.Metadata, options: BlobTargetOption*): Task[Blob] = {
    val update = BlobInfo.toJava(underlying.getBucket, underlying.getName, metadata)
    Task(underlying.getStorage.update(update, options: _*))
      .map(Blob.apply)
  }

  def delete(options: BlobSourceOption*): Task[Boolean] =
    Task(underlying.delete(options: _*))

  /**
   * Copies this blob to the target Blob.
   *
   * $copyToNote
   */
  def copyTo(targetBlob: BlobId, options: BlobSourceOption*): Task[Blob] =
    Task.evalAsync(underlying.copyTo(targetBlob, options: _*))
      .map(_.getResult)
      .map(Blob.apply)

  /**
   * Copies this blob to the target Bucket.
   *
   * $copyToNote
   */
  def copyTo(targetBucket: String, options: BlobSourceOption*): Task[Blob] =
    Task.evalAsync(underlying.copyTo(targetBucket, options: _*))
      .map(_.getResult)
      .map(Blob.apply)

  /**
   * Copies this blob to the target Blob in the target Bucket.
   *
   * $copyToNote
   */
  def copyTo(targetBucket: String, targetBlob: String, options: BlobSourceOption*): Task[Blob] =
    Task.evalAsync(underlying.copyTo(targetBucket, targetBlob, options: _*))
      .map(_.getResult)
      .map(Blob.apply)

  /**
   * Generates a signed URL for this blob. If you want to allow access for a fixed amount of time to
   * this blob, you can use this method to generate a URL that is only valid within a certain time
   * period. This is particularly useful if you don't want publicly accessible blobs, but also don't
   * want to require users to explicitly log in. Signing a URL requires a service account signer.
   *
   * If an instance of [[com.google.auth.ServiceAccountSigner]] was passed to [[com.google.cloud.storage.StorageOptions]]
   * builder via [[com.google.cloud.storage.StorageOptions#setCredentials]] or the default credentials are being
   * used and the environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is set or your application is running in
   * App Engine, then this function will use those credentials to sign the URL.
   *
   * If the credentials passed to [[com.google.cloud.storage.StorageOptions]] do not implement
   * [[com.google.auth.ServiceAccountSigner]] (this is the case, for instance, for Compute Engine credentials and
   * Google Cloud SDK credentials) then this function will throw an [[IllegalStateException]] unless an implementation
   * of [[com.google.auth.ServiceAccountSigner]] is passed using the [[SignUrlOption#signWith(ServiceAccountSigner)]]
   * option.
   */
  def signUrl(duration: FiniteDuration, options: SignUrlOption*): Task[URL] =
    Task(underlying.signUrl(duration.toMillis, TimeUnit.MILLISECONDS, options: _*))

  /**
   * Creates a new ACL entry on this Blob.
   */
  def createAcl(acl: Acl): Task[Acl] =
    Task(underlying.createAcl(acl))

  /**
   * Returns the [[Acl]] entry for the specified entity on this [[Blob]] or [[None]] if not found.
   */
  def getAcl(acl: Acl.Entity): Task[Option[Acl]] =
    Task(underlying.getAcl(acl)).map(Option(_))

  /**
   * Updates an ACL entry on this [[Blob]].
   */
  def updateAcl(acl: Acl): Task[Acl] =
    Task(underlying.updateAcl(acl))

  /**
   * Deletes the [[Acl]] entry for the specified [[com.google.cloud.storage.Acl.Entity]] on this [[Blob]].
   */
  def deleteAcl(acl: Acl.Entity): Task[Boolean] =
    Task(underlying.deleteAcl(acl))

  /**
   * Returns an [[Observable]] of all the [[Acl]] Entries for this [[Blob]].
   */
  def listAcls(): Observable[Acl] = {
    Observable.suspend {
      Observable.fromIterable(underlying.listAcls().asScala)
    }
  }

  /**
   * Returns all the metadata associated with this Blob instance.
   */
  def blobInfo: BlobInfo = BlobInfo.fromJava(underlying)
}

object Blob {
  def apply(blob: google.Blob) = new Blob(blob)
}