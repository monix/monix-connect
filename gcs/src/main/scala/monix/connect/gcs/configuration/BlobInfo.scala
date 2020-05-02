package monix.connect.gcs.configuration

import java.time.Instant

import com.google.cloud.storage.BlobInfo.CustomerEncryption
import com.google.cloud.storage.{Acl, BlobId, BlobInfo => GoogleBlobInfo, StorageClass}

import scala.jdk.CollectionConverters._

final case class BlobInfo(name: String,
                          bucket: String,
                          generatedId: String,
                          cacheControl: Option[String],
                          size: Long,
                          contentType: Option[String],
                          contentEncoding: Option[String],
                          contentDisposition: Option[String],
                          contentLanguage: Option[String],
                          componentCount: Int,
                          etag: Option[String],
                          md5: Option[String],
                          md5ToHexString: Option[String],
                          crc32c: Option[String],
                          crc32cToHexString: Option[String],
                          mediaLink: Option[String],
                          metadata: Map[String, String],
                          generation: Long,
                          metageneration: Long,
                          deleteTime: Instant,
                          updateTime: Instant,
                          createTime: Instant,
                          isDirectory: Boolean,
                          customerEncryption: Option[CustomerEncryption],
                          storageClass: StorageClass,
                          kmsKeyName: String,
                          eventBasedHold: Option[Boolean],
                          temporaryHold: Option[Boolean],
                          retentionExpirationTime: Option[Instant])


object BlobInfo {

  final case class Metadata(contentType: Option[String] = None,
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
                            temporaryHold: Option[Boolean] = None)

  private[gcs] def toJava(bucket: String, name: String, info: BlobInfo.Metadata): GoogleBlobInfo = {
    val builder = GoogleBlobInfo.newBuilder(BlobId.of(bucket, name))
    info.contentType.foreach(builder.setContentType)
    info.contentDisposition.foreach(builder.setContentDisposition)
    info.contentLanguage.foreach(builder.setContentLanguage)
    info.contentEncoding.foreach(builder.setContentEncoding)
    info.cacheControl.foreach(builder.setCacheControl)
    info.crc32c.foreach(builder.setCrc32c)
    info.crc32cFromHexString.foreach(builder.setCrc32cFromHexString)
    info.md5.foreach(builder.setMd5)
    info.md5FromHexString.foreach(builder.setMd5FromHexString)
    info.storageClass.foreach(builder.setStorageClass)
    info.temporaryHold.foreach(b => builder.setEventBasedHold(b))
    info.eventBasedHold.foreach(b => builder.setEventBasedHold(b))
    builder.setAcl(info.acl.asJava)
    builder.setMetadata(info.metadata.asJava)
    builder.build()
  }

  private[gcs] def fromJava(info: GoogleBlobInfo): BlobInfo = {
    BlobInfo(
      name = info.getName,
      bucket = info.getBucket,
      generatedId = info.getGeneratedId,
      cacheControl = Option(info.getCacheControl),
      size = info.getSize(),
      contentType = Option(info.getContentType),
      contentEncoding = Option(info.getContentEncoding),
      contentDisposition = Option(info.getContentDisposition),
      contentLanguage = Option(info.getContentLanguage),
      componentCount = info.getComponentCount,
      etag = Option(info.getEtag),
      md5 = Option(info.getMd5),
      md5ToHexString = Option(info.getMd5ToHexString),
      crc32c = Option(info.getCrc32c),
      crc32cToHexString = Option(info.getCrc32cToHexString),
      mediaLink = Option(info.getMediaLink),
      metadata = Option(info.getMetadata).map(_.asScala.toMap).getOrElse {
        Map.empty[String, String]
      },
      generation = info.getGeneration,
      metageneration = info.getMetageneration,
      deleteTime = Instant.ofEpochMilli(info.getDeleteTime),
      updateTime = Instant.ofEpochMilli(info.getUpdateTime),
      createTime = Instant.ofEpochMilli(info.getCreateTime),
      isDirectory = info.isDirectory,
      customerEncryption = Option(info.getCustomerEncryption),
      storageClass = info.getStorageClass,
      kmsKeyName = info.getKmsKeyName,
      eventBasedHold = Option(info.getEventBasedHold),
      temporaryHold = Option(info.getTemporaryHold),
      retentionExpirationTime = Option(info.getRetentionExpirationTime).map(Instant.ofEpochMilli(_)),
    )
  }
}