package io.monix.connect.gcs.configuration

import com.google.cloud.storage.{Acl, BlobId, StorageClass}
import com.google.cloud.{storage => gcs}

import scala.jdk.CollectionConverters._

final case class BlobInfo(contentType: Option[String] = None,
                          contentDisposition: Option[String] = None,
                          contentLanguage: Option[String] = None,
                          contentEncoding: Option[String] = None,
                          cacheControl: Option[String] = None,
                          crc32c: Option[String] = None,
                          crc32cFromHexString: Option[String] = None,
                          md5: Option[String] = None,
                          md5FromHexString: Option[String] = None,
                          metadata: Map[String, String] = Map.empty[String, String],
                          storageClass: Option[StorageClass] = None,
                          acl: List[Acl] = List.empty[Acl],
                          eventBasedHold: Option[Boolean] = None,
                          temporaryHold: Option[Boolean] = None) {


  def toBlobInfo(blobId: BlobId): gcs.BlobInfo = {
    val builder = gcs.BlobInfo.newBuilder(blobId)

    // Content
    contentType.foreach(builder.setContentType)
    contentDisposition.foreach(builder.setContentDisposition)
    contentLanguage.foreach(builder.setContentLanguage)
    contentEncoding.foreach(builder.setContentEncoding)
    cacheControl.foreach(builder.setCacheControl)

    // CRC
    crc32c.foreach(builder.setCrc32c)
    crc32cFromHexString.foreach(builder.setCrc32cFromHexString)

    // MD5
    md5.foreach(builder.setMd5)
    md5FromHexString.foreach(builder.setMd5FromHexString)

    storageClass.foreach(builder.setStorageClass)
    temporaryHold.foreach(b => builder.setEventBasedHold(b))
    eventBasedHold.foreach(b => builder.setEventBasedHold(b))

    // Iterables
    builder.setAcl(acl.asJava)
    builder.setMetadata(metadata.asJava)

    builder.build()
  }
}

object BlobInfo {
  def fromBlobId(blobId: BlobId): gcs.BlobInfo =
    gcs.BlobInfo.newBuilder(blobId).build()
}
