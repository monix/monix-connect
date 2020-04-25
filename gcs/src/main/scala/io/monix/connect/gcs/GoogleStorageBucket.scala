package io.monix.connect.gcs

import java.io.File

import com.google.cloud.storage.{Bucket, BucketInfo, Storage, StorageOptions}
import io.monix.connect.gcs.configuration.GoogleStorageBucketConfig
import monix.eval.Task

import scala.jdk.CollectionConverters._

final class GoogleStorageBucket(storage: Storage, bucket: Bucket) {

  def upload(file: File) = ???

}

object GoogleStorageBucket {

  private def getBucketInfo(config: GoogleStorageBucketConfig): Task[BucketInfo] = Task {
    // Important
    val builder = BucketInfo.newBuilder(config.name)
    config.location.foreach(location => builder.setLocation(location))
    config.storageClass.foreach(sc => builder.setStorageClass(sc))
    config.versioningEnabled.foreach(b => builder.setVersioningEnabled(b))
    config.retentionPeriod.foreach(rp => builder.setRetentionPeriod(rp.toMillis))
    config.requesterPays.foreach(b => builder.setRequesterPays(b))
    config.logging.foreach(l => builder.setLogging(l))
    config.defaultEventBasedHold.foreach(evb => builder.setDefaultEventBasedHold(evb))

    // Security and Access Control
    builder.setAcl(config.acl.asJava)
    builder.setDefaultAcl(config.defaultAcl.asJava)
    builder.setCors(config.cors.asJava)
    builder.setLifecycleRules(config.lifecycleRules.asJava)
    config.iamConfiguration.foreach(iam => builder.setIamConfiguration(iam))
    config.defaultKmsKeyName.foreach(kms => builder.setDefaultKmsKeyName(kms))

    // Pages and Metadata
    builder.setLabels(config.labels.asJava)
    config.indexPage.foreach(builder.setNotFoundPage)
    config.notFoundPage.foreach(builder.setNotFoundPage)

    builder.build()
  }

  // TODO: Add alternative authentication methods.
  private def getStorageInstance(): Task[Storage] = {
    Task(StorageOptions.getDefaultInstance.getService)
  }

  // TODO: Handle Options
  private def getBucketInstance(storage: Storage, bucketInfo: BucketInfo): Task[Bucket] = Task {
    storage.create(bucketInfo)
  }

  def apply(config: GoogleStorageBucketConfig): Task[GoogleStorageBucket] = {
    for {
      storage    <- getStorageInstance()
      bucketInfo <- getBucketInfo(config)
      bucket     <- getBucketInstance(storage, bucketInfo)
    } yield new GoogleStorageBucket(storage, bucket)
  }
}