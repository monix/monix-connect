/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

import com.google.cloud.storage.BucketInfo.{IamConfiguration, LifecycleRule, Logging}
import com.google.cloud.storage.{Acl, BucketInfo, Cors, StorageClass}
import GcsBucketInfo.Locations.Location

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * This class wraps the [[com.google.cloud.storage.Blob]] class, providing an idiomatic scala API
  * handling null values with [[Option]] where applicable, as well as wrapping all side-effectful calls
  */
object GcsBucketInfo {

  def fromJava(bucketInfo: BucketInfo): GcsBucketInfo = {
    val requestPays = Option(bucketInfo.requesterPays()).map(_.booleanValue)
    val versioningEnabled = Option(bucketInfo.versioningEnabled).map(_.booleanValue)
    val metageneration = Option(bucketInfo.getMetageneration).map(_.longValue)
    val defaultEventBaseHold = Option(bucketInfo.getDefaultEventBasedHold).map(_.booleanValue)
    val retentionEffectiveTime = Option(bucketInfo.getRetentionEffectiveTime).map(_.longValue)
    val retentionPolicyIsLocked = Option(bucketInfo.retentionPolicyIsLocked).map(_.booleanValue)
    val retentionPeriod = Option(bucketInfo.getRetentionPeriod).map(_.longValue)

    GcsBucketInfo(
      generatedId = bucketInfo.getGeneratedId,
      name = bucketInfo.getName,
      owner = bucketInfo.getOwner,
      selfLink = bucketInfo.getSelfLink,
      requesterPays = requestPays,
      versioningEnabled = versioningEnabled,
      indexPage = bucketInfo.getIndexPage,
      notFoundPage = bucketInfo.getNotFoundPage,
      lifecycleRules = Option(bucketInfo.getLifecycleRules).map(_.asScala.toList).getOrElse(List.empty[LifecycleRule]),
      storageClass = Option(bucketInfo.getStorageClass),
      location = bucketInfo.getLocation,
      etag = bucketInfo.getEtag,
      createTime = bucketInfo.getCreateTime,
      metageneration = metageneration,
      cors = Option(bucketInfo.getCors).map(_.asScala.toList).getOrElse(List.empty[Cors]),
      acl = Option(bucketInfo.getAcl).map(_.asScala.toList).getOrElse(List.empty[Acl]),
      defaultAcl = Option(bucketInfo.getDefaultAcl).map(_.asScala.toList).getOrElse(List.empty[Acl]),
      labels = Option(bucketInfo.getLabels).map(_.asScala.toMap).getOrElse(Map.empty[String, String]),
      defaultKmsKeyName = bucketInfo.getDefaultKmsKeyName,
      defaultEventBasedHold = defaultEventBaseHold,
      retentionEffectiveTime = retentionEffectiveTime,
      retentionPolicyIsLocked = retentionPolicyIsLocked,
      retentionPeriod = retentionPeriod,
      iamConfiguration = bucketInfo.getIamConfiguration,
      locationType = bucketInfo.getLocationType,
      logging = bucketInfo.getLogging
    )
  }

  def withMetadata(bucketName: String, location: Location, metadata: Option[Metadata]): BucketInfo = {
    val builder = BucketInfo.newBuilder(bucketName).setLocation(location.toString)
    metadata.foreach(_.storageClass.foreach(builder.setStorageClass))
    metadata.foreach(_.logging.foreach(builder.setLogging))
    metadata.foreach(_.retentionPeriod.foreach(rp => builder.setRetentionPeriod(rp.toMillis)))

    // Booleans
    metadata.foreach(_.versioningEnabled.foreach(builder.setVersioningEnabled(_)))
    metadata.foreach(_.requesterPays.foreach(builder.setRequesterPays(_)))
    metadata.foreach(_.defaultEventBasedHold.foreach(builder.setDefaultEventBasedHold(_)))

    // Security and Access Control
    metadata.foreach(md => builder.setAcl(md.acl.asJava))
    metadata.foreach(md => builder.setDefaultAcl(md.defaultAcl.asJava))
    metadata.foreach(md => builder.setCors(md.cors.asJava))
    metadata.foreach(md => builder.setLifecycleRules(md.lifecycleRules.asJava))
    metadata.foreach(_.iamConfiguration.foreach(builder.setIamConfiguration))
    metadata.foreach(_.defaultKmsKeyName.foreach(builder.setDefaultKmsKeyName))

    // Pages and Metadata
    metadata.foreach(md => builder.setLabels(md.labels.asJava))
    metadata.foreach(_.indexPage.foreach(builder.setIndexPage))
    metadata.foreach(_.notFoundPage.foreach(builder.setNotFoundPage))

    builder.build()
  }

  object Locations {

    type Location = String

    // Regions
    val `NORTHAMERICA-NORTHEAST1`: Location = "NORTHAMERICA-NORTHEAST1"
    val `US-CENTRAL1`: Location = "US-CENTRAL1"
    val `US-EAST1`: Location = "US-EAST1"
    val `US-EAST4`: Location = "US-EAST4"
    val `US-WEST1`: Location = "US-WEST1"
    val `US-WEST2`: Location = "US-WEST2"
    val `US-WEST3`: Location = "US-WEST3"
    val `US-WEST4`: Location = "US-WEST4"
    val `SOUTHAMERICA-EAST1`: Location = "SOUTHAMERICA-EAST1"
    val `EUROPE-NORTH1`: Location = "EUROPE-NORTH1"
    val `EUROPE-WEST1`: Location = "EUROPE-WEST1"
    val `EUROPE-WEST2`: Location = "EUROPE-WEST2"
    val `EUROPE-WEST3`: Location = "EUROPE-WEST3"
    val `EUROPE-WEST4`: Location = "EUROPE-WEST4"
    val `EUROPE-WEST6`: Location = "EUROPE-WEST6"
    val `ASIA-EAST1`: Location = "ASIA-EAST1"
    val `ASIA-EAST2`: Location = "ASIA-EAST2"
    val `ASIA-NORTHEAST1`: Location = "ASIA-NORTHEAST1"
    val `ASIA-NORTHEAST2`: Location = "ASIA-NORTHEAST2"
    val `ASIA-NORTHEAST3`: Location = "ASIA-NORTHEAST3"
    val `ASIA-SOUTH1`: Location = "ASIA-SOUTH1"
    val `ASIA-SOUTHEAST1`: Location = "ASIA-SOUTHEAST1"
    val `AUSTRALIA-SOUTHEAST1`: Location = "AUSTRALIA-SOUTHEAST1"

    // Multi-regions
    val ASIA: Location = "ASIA"
    val EU: Location = "EU"
    val US: Location = "US"

    // Dual-regions
    val EUR4: Location = "EUR4"
    val NAM4: Location = "NAM4"
  }

  final case class Metadata(
    labels: Map[String, String] = Map.empty[String, String],
    requesterPays: Option[Boolean] = None,
    versioningEnabled: Option[Boolean] = None,
    storageClass: Option[StorageClass] = None,
    retentionPeriod: Option[FiniteDuration] = None,
    acl: List[Acl] = List.empty[Acl],
    cors: List[Cors] = List.empty[Cors],
    defaultAcl: List[Acl] = List.empty[Acl],
    lifecycleRules: List[LifecycleRule] = List.empty[LifecycleRule],
    logging: Option[BucketInfo.Logging] = None,
    indexPage: Option[String] = None,
    notFoundPage: Option[String] = None,
    defaultKmsKeyName: Option[String] = None,
    defaultEventBasedHold: Option[Boolean] = None,
    iamConfiguration: Option[IamConfiguration] = None)
}

/**
  * A safe and scala idiomatic way of accessing to the bucket information since it provides
  * conversions from the java language to scala and returning empty options instead of null values.
  */
private[storage] case class GcsBucketInfo(
  name: String,
  location: String,
  owner: Acl.Entity,
  selfLink: String,
  requesterPays: Option[Boolean],
  versioningEnabled: Option[Boolean],
  indexPage: String,
  notFoundPage: String,
  lifecycleRules: List[LifecycleRule],
  storageClass: Option[StorageClass],
  etag: String,
  createTime: Long,
  metageneration: Option[Long],
  cors: List[Cors],
  acl: List[Acl],
  defaultAcl: List[Acl],
  labels: Map[String, String],
  defaultKmsKeyName: String,
  defaultEventBasedHold: Option[Boolean],
  retentionEffectiveTime: Option[Long],
  retentionPolicyIsLocked: Option[Boolean],
  retentionPeriod: Option[Long],
  iamConfiguration: IamConfiguration,
  locationType: String,
  logging: Logging,
  generatedId: String)
