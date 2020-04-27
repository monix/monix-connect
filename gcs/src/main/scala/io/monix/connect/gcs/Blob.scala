package io.monix.connect.gcs

import java.lang
import java.net.URL
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.TimeUnit

import com.google.cloud.storage.Blob.BlobSourceOption
import com.google.cloud.storage.Storage.{BlobTargetOption, SignUrlOption}
import com.google.cloud.storage.{Acl, BlobId, BlobInfo, StorageClass, Blob => GoogleBlob, Option => _}
import com.google.cloud.{storage => google}
import io.monix.connect.gcs.configuration.BlobConfig
import io.monix.connect.gcs.utiltiies.StorageDownloader
import monix.eval.Task

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
final class Blob(underlying: GoogleBlob) extends StorageDownloader {

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
   * Updates the blob's information. Bucket or blob's name cannot be changed by this method. If you
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
  def update(config: BlobConfig, options: BlobTargetOption*): Task[Blob] = {
    val update = config.toBlobInfo(underlying.getBlobId)
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
   * TODO: Documentation
   */
  def downloadTo(path: Path, chunkSize: Int = 4096): Task[Unit] =
    downloadFromBucket(underlying.getStorage, underlying.getBucket, underlying.getBlobId, path, chunkSize)

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



  // TODO: Document use case for below functions, retrieving blob metadata is unsafe due to the usage of null.
  // ------------------------------------------------------------------------------- //
  def generatedId: String =
    underlying.getGeneratedId

  def cacheControl: Option[String] =
    Option(underlying.getCacheControl)

  def getAcl: List[Acl] =
    underlying.getAcl().asScala.toList

  def getOwner: Acl.Entity =
    underlying.getOwner

  def getSize: lang.Long =
    underlying.getSize

  def getContentType: Option[String] =
    Option(underlying.getContentType)

  def getContentEncoding: Option[String] =
    Option(underlying.getContentEncoding)

  def getContentDisposition: Option[String] =
    Option(underlying.getContentDisposition)

  def getContentLanguage: Option[String] =
    Option(underlying.getContentLanguage)

  def getComponentCount: Int =
    underlying.getComponentCount

  def getEtag: String =
    underlying.getEtag

  def getMd5: Option[String] =
    Option(underlying.getMd5)

  def getMd5ToHexString: Option[String] =
    Option(underlying.getMd5ToHexString)

  def getCrc32c: Option[String] =
    Option(underlying.getCrc32c)

  def getCrc32cToHexString: Option[String] =
    Option(underlying.getCrc32cToHexString)

  def getMediaLink: URL =
    new URL(underlying.getMediaLink)

  def getMetadata: Map[String, String] =
    Option(underlying.getMetadata)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])

  def getGeneration: Long =
    underlying.getGeneration

  def getMetageneration: Long =
    underlying.getMetageneration

  def getDeleteTime: Instant =
    Instant.ofEpochMilli(underlying.getDeleteTime)

  def getUpdateTime: Instant =
    Instant.ofEpochMilli(underlying.getUpdateTime)

  def getCreateTime: Instant =
    Instant.ofEpochMilli(underlying.getCreateTime)

  def isDirectory: Boolean =
    underlying.isDirectory

  def getCustomerEncryption: BlobInfo.CustomerEncryption =
    underlying.getCustomerEncryption

  def getStorageClass: StorageClass =
    underlying.getStorageClass

  def getKmsKeyName: String =
    underlying.getKmsKeyName

  def getEventBasedHold: Option[Boolean] =
    Option(underlying.getEventBasedHold)

  def getTemporaryHold: Option[lang.Boolean] =
    Option(underlying.getTemporaryHold)

  def getRetentionExpirationTime: Option[Instant] =
    Option(underlying.getRetentionExpirationTime)
      .map(ts => Instant.ofEpochMilli(ts))
}

object Blob {
  def apply(blob: google.Blob) = new Blob(blob)
}