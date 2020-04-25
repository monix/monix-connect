package io.monix.connect.gcs

import java.lang
import java.net.URL
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.TimeUnit

import com.google.cloud.storage.{Option => _, _}
import com.google.cloud.{storage => google}
import io.monix.connect.gcs.streaming.StorageDownloader
import monix.eval.Task

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

final class Blob(blob: google.Blob) extends StorageDownloader {

  implicit val s: Storage = blob.getStorage

  def downloadTo(path: Path, chunkSize: Int = 4096): Task[Unit] =
    downloadFromBucket(blob.getBlobId, path, chunkSize)

  def exists(options: google.Blob.BlobSourceOption*): Task[Boolean] =
    Task(blob.exists(options: _*))

  def reload(options: google.Blob.BlobSourceOption*): Task[Unit] =
    Task(blob.reload(options: _*))

  def delete(options: google.Blob.BlobSourceOption*): Task[Boolean] =
    Task(blob.delete(options: _*))

  def copyTo(targetBlob: BlobId, options: google.Blob.BlobSourceOption*): Task[Blob] =
    Task(blob.copyTo(targetBlob, options: _*)).map(_.getResult).map(Blob.apply)

  def copyTo(targetBucket: String, options: google.Blob.BlobSourceOption*): Task[Blob] =
    Task(blob.copyTo(targetBucket, options: _*)).map(_.getResult).map(Blob.apply)

  def copyTo(targetBucket: String, targetBlob: String, options: google.Blob.BlobSourceOption*): Task[Blob] =
    Task(blob.copyTo(targetBucket, targetBlob, options: _*)).map(_.getResult).map(Blob.apply)

  def signUrl(duration: FiniteDuration, options: Storage.SignUrlOption*): Task[URL] =
    Task(blob.signUrl(duration.toMillis, TimeUnit.MILLISECONDS, options: _*))


  // ------------------------------------------------------------------------------- //
  def generatedId: String = {
    blob.getGeneratedId
  }

  def cacheControl: Option[String] =
    Option(blob.getCacheControl)

  def acl: List[Acl] =
    blob.getAcl.asScala.toList

  def owner: Acl.Entity =
    blob.getOwner

  def size: lang.Long =
    blob.getSize

  def contentType: Option[String] =
    Option(blob.getContentType)

  def contentEncoding: Option[String] =
    Option(blob.getContentEncoding)

  def contentDisposition: Option[String] =
    Option(blob.getContentDisposition)

  def contentLanguage: Option[String] =
    Option(blob.getContentLanguage)

  def componentCount: Int =
    blob.getComponentCount

  def eTag(): String =
    blob.getEtag

  def md5(): Option[String] =
    Option(blob.getMd5)

  def md5ToHexString: Option[String] =
    Option(blob.getMd5ToHexString)

  def crc32c: Option[String] =
    Option(blob.getCrc32c)

  def crc32cToHexString: Option[String] =
    Option(blob.getCrc32cToHexString)

  def mediaLink: URL =
    new URL(blob.getMediaLink)

  def metadata: Map[String, String] =
    Option(blob.getMetadata)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])

  def generation: Long =
    blob.getGeneration

  def metaGeneration: Long =
    blob.getMetageneration

  def deletedAt: Instant =
    Instant.ofEpochMilli(blob.getDeleteTime)

  def updatedAt: Instant =
    Instant.ofEpochMilli(blob.getUpdateTime)

  def createdAt: Instant =
    Instant.ofEpochMilli(blob.getCreateTime)

  def isDirectory: Boolean =
    blob.isDirectory

  def customerEncryption: BlobInfo.CustomerEncryption =
    blob.getCustomerEncryption

  def storageClass: StorageClass =
    blob.getStorageClass

  def kmsKeyName: String = {
    blob.getKmsKeyName
  }

  def eventBasedHold: Option[Boolean] =
    Option(blob.getEventBasedHold)

  def temporaryHold: Option[lang.Boolean] =
    Option(blob.getTemporaryHold)

  def retentionExpirationTime: Option[Instant] =
    Option(blob.getRetentionExpirationTime)
      .map(ts => Instant.ofEpochMilli(ts))
}

object Blob {
  def apply(blob: google.Blob) = new Blob(blob)
}