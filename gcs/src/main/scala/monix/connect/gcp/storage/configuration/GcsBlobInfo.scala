/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.gcp.storage.configuration

import java.time.Instant

import com.google.cloud.storage.BlobInfo.CustomerEncryption
import com.google.cloud.storage.{Acl, BlobId, BlobInfo, StorageClass}

import scala.jdk.CollectionConverters._

/** Object that provides builder and conversion (from java) methods for [[GcsBlobInfo]]. */
object GcsBlobInfo {

  private[storage] def withMetadata(name: String, blobName: String, metadata: Option[Metadata]): BlobInfo = {
    val builder = BlobInfo.newBuilder(BlobId.of(name, blobName))
    metadata.foreach { options =>
      options.contentType.foreach(builder.setContentType)
      options.contentDisposition.foreach(builder.setContentDisposition)
      options.contentLanguage.foreach(builder.setContentLanguage)
      options.contentEncoding.foreach(builder.setContentEncoding)
      options.cacheControl.foreach(builder.setCacheControl)
      options.crc32c.foreach(x => builder.setCrc32c(x))
      options.crc32cFromHexString.foreach(x => builder.setCrc32cFromHexString(x))
      options.md5.foreach(x => builder.setMd5(x))
      options.md5FromHexString.foreach(x => builder.setMd5FromHexString(x))
      options.storageClass.foreach(builder.setStorageClass)
      options.temporaryHold.foreach(builder.setTemporaryHold(_))
      options.eventBasedHold.foreach(builder.setEventBasedHold(_))
      builder.setAcl(options.acl.asJava)
      builder.setMetadata(options.metadata.asJava)
    }

    builder.build()
  }

  /** Converter from the google's java [[BlobInfo]] to monix-conect's scala [[GcsBlobInfo]]  */
  def fromJava(blobInfo: BlobInfo): GcsBlobInfo = {
    /**
      * These fields can't be initialized directly below when creating other fields since
      * the default value of the option type would be applied, thus the option would not be `None`.
      */
    val cacheControl = Option(blobInfo.getCacheControl)
    val componentCount = Option(blobInfo.getComponentCount).map(_.intValue())
    val generation = Option(blobInfo.getGeneration).map(_.longValue)
    val metaGeneration = Option(blobInfo.getMetageneration).map(_.longValue)
    val temporaryHold = Option(blobInfo.getTemporaryHold).map(_.booleanValue())
    val eventBasedHold = Option(blobInfo.getEventBasedHold).map(_.booleanValue())
    GcsBlobInfo(
      name = blobInfo.getName,
      bucket = blobInfo.getBucket,
      generatedId = Option(blobInfo.getGeneratedId),
      selfLink = Option(blobInfo.getSelfLink),
      cacheControl = cacheControl,
      acl = Option(blobInfo.getAcl).map(_.asScala.toList).getOrElse {
        List.empty[Acl]
      },
      owner = Option(blobInfo.getOwner),
      size = Option(blobInfo.getSize()),
      contentType = Option(blobInfo.getContentType),
      contentEncoding = Option(blobInfo.getContentEncoding),
      contentDisposition = Option(blobInfo.getContentDisposition),
      contentLanguage = Option(blobInfo.getContentLanguage),
      componentCount = componentCount,
      etag = Option(blobInfo.getEtag),
      md5 = Option(blobInfo.getMd5),
      //md5ToHexString = Option(blobInfo.getMd5ToHexString), todo
      crc32c = Option(blobInfo.getCrc32c),
      //crc32cToHexString = Option(blobInfo.getCrc32cToHexString), todo
      mediaLink = Option(blobInfo.getMediaLink),
      metadata = Option(blobInfo.getMetadata).map(_.asScala.toMap).getOrElse {
        Map.empty[String, String]
      },
      generation = generation,
      metageneration = metaGeneration,
      deleteTime = Option(blobInfo.getDeleteTime).map(Instant.ofEpochMilli(_)),
      updateTime = Option(blobInfo.getUpdateTime).map(Instant.ofEpochMilli(_)),
      createTime = Option(blobInfo.getUpdateTime).map(Instant.ofEpochMilli(_)),
      isDirectory = Option(blobInfo.isDirectory),
      customerEncryption = Option(blobInfo.getCustomerEncryption),
      storageClass = Option(blobInfo.getStorageClass),
      kmsKeyName = Option(blobInfo.getKmsKeyName),
      eventBasedHold = eventBasedHold,
      temporaryHold = temporaryHold,
      retentionExpirationTime = Option(blobInfo.getRetentionExpirationTime).map(Instant.ofEpochMilli(_))
    )
  }

  final case class Metadata(
    contentType: Option[String] = None,
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
}

/**
  * A safe and scala idiomatic way of accessing to the blob information, since it provides
  * conversions from the java language to scala and returning empty options instead of null values.
  */
private[storage] case class GcsBlobInfo(
  name: String,
  bucket: String,
  generatedId: Option[String] = None,
  selfLink: Option[String] = None,
  cacheControl: Option[String] = None,
  acl: List[Acl] = List.empty[Acl],
  owner: Option[Acl.Entity] = None,
  size: Option[Long] = None,
  contentType: Option[String] = None,
  contentEncoding: Option[String] = None,
  contentDisposition: Option[String] = None,
  contentLanguage: Option[String] = None,
  componentCount: Option[Int] = None,
  etag: Option[String] = None,
  md5: Option[String] = None,
  md5ToHexString: Option[String] = None,
  crc32c: Option[String] = None,
  crc32cToHexString: Option[String] = None,
  mediaLink: Option[String] = None,
  metadata: Map[String, String] = Map.empty,
  generation: Option[Long] = None,
  metageneration: Option[Long] = None,
  deleteTime: Option[Instant] = None,
  updateTime: Option[Instant] = None,
  createTime: Option[Instant] = None,
  isDirectory: Option[Boolean] = None,
  customerEncryption: Option[CustomerEncryption] = None,
  storageClass: Option[StorageClass] = None,
  kmsKeyName: Option[String] = None,
  eventBasedHold: Option[Boolean] = None,
  temporaryHold: Option[Boolean] = None,
  retentionExpirationTime: Option[Instant] = None)
