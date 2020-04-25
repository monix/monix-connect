package io.monix.connect.gcs

import java.io.File

import com.google.cloud.storage.{Option => _, Bucket => _, _}
import com.google.cloud.storage
import io.monix.connect.gcs.configuration.{BlobInfo, BucketConfig}
import monix.eval.Task

import scala.jdk.CollectionConverters._

final class Bucket(storage: Storage, bucket: storage.Bucket, writeBufferSize: Int, readBufferSize: Int)
  extends StorageUploader {

  implicit val s: Storage = storage
  implicit val b: storage.Bucket = bucket

  def getBlob(name: String): Task[Blob] =
    Task(storage.get(BlobId.of(bucket.getName, name)))

  def upload(file: File, config: Option[BlobInfo]): Task[Blob] = {
    val blobId = BlobId.of(bucket.getName, file.getName)
    val blobInfo = config.map(_.toBlobInfo(blobId))
      .getOrElse(BlobInfo.fromBlobId(blobId))

    uploadToBucket(blobInfo, file, writeBufferSize)
  }
}

object Bucket {

  private def getBucketInfo(config: BucketConfig): Task[BucketInfo] = Task {
    val builder = BucketInfo.newBuilder(config.name)
    config.location.foreach(builder.setLocation)
    config.storageClass.foreach(builder.setStorageClass)
    config.versioningEnabled.foreach(b => builder.setVersioningEnabled(b))
    config.retentionPeriod.foreach(rp => builder.setRetentionPeriod(rp.toMillis))
    config.requesterPays.foreach(b => builder.setRequesterPays(b))
    config.logging.foreach(builder.setLogging)
    config.defaultEventBasedHold.foreach(evb => builder.setDefaultEventBasedHold(evb))

    // Security and Access Control
    builder.setAcl(config.acl.asJava)
    builder.setDefaultAcl(config.defaultAcl.asJava)
    builder.setCors(config.cors.asJava)
    builder.setLifecycleRules(config.lifecycleRules.asJava)
    config.iamConfiguration.foreach(builder.setIamConfiguration)
    config.defaultKmsKeyName.foreach(builder.setDefaultKmsKeyName)

    // Pages and Metadata
    builder.setLabels(config.labels.asJava)
    config.indexPage.foreach(builder.setNotFoundPage)
    config.notFoundPage.foreach(builder.setNotFoundPage)

    builder.build()
  }

  // TODO: Add alternative authentication methods.
  private def getStorageInstance(): Task[Storage] =
    Task(StorageOptions.getDefaultInstance.getService)

  // TODO: Handle Options
  private def getBucketInstance(storage: Storage, bucketInfo: BucketInfo): Task[Bucket] =
    Task(storage.create(bucketInfo))

  // TODO: Check if bucket exists before creating.
  def apply(config: BucketConfig): Task[Bucket] = {
    for {
      storage    <- getStorageInstance()
      bucketInfo <- getBucketInfo(config)
      bucket     <- getBucketInstance(storage, bucketInfo)
    } yield new Bucket(storage, bucket, config.writeBufferSize, config.readBufferSize)
  }
}