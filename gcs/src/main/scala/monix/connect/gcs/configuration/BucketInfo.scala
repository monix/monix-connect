package monix.connect.gcs.configuration

import com.google.cloud.storage.BucketInfo.{IamConfiguration, LifecycleRule, Logging}
import com.google.cloud.storage.{Acl, Cors, StorageClass, BucketInfo => GoogleBucketInfo}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

object BucketInfo {
  def apply(
    name: String,
    location: String,
    owner: Acl.Entity,
    selfLink: String,
    requesterPays: Option[Boolean],
    versioningEnabled: Option[Boolean],
    indexPage: String,
    notFoundPage: String,
    lifecycleRules: List[LifecycleRule],
    storageClass: StorageClass,
    etag: String,
    createTime: Long,
    metageneration: Long,
    cors: List[Cors],
    acl: List[Acl],
    defaultAcl: List[Acl],
    labels: Map[String, String],
    defaultKmsKeyName: String,
    defaultEventBasedHold: Option[Boolean],
    retentionEffectiveTime: Long,
    retentionPolicyIsLocked: Option[Boolean],
    retentionPeriod: Long,
    iamConfiguration: IamConfiguration,
    locationType: String,
    logging: Logging,
    generatedId: String): BucketInfo =
    new BucketInfo(
      name,
      location,
      owner,
      selfLink,
      requesterPays,
      versioningEnabled,
      indexPage,
      notFoundPage,
      lifecycleRules,
      storageClass,
      etag,
      createTime,
      metageneration,
      cors,
      acl,
      defaultAcl,
      labels,
      defaultKmsKeyName,
      defaultEventBasedHold,
      retentionEffectiveTime,
      retentionPolicyIsLocked,
      retentionPeriod,
      iamConfiguration,
      locationType,
      logging,
      generatedId)

  def fromJava(info: GoogleBucketInfo): BucketInfo = {
    BucketInfo(
      generatedId = info.getGeneratedId,
      name = info.getName,
      owner = info.getOwner,
      selfLink = info.getSelfLink,
      requesterPays = Option(info.requesterPays()),
      versioningEnabled = Option(info.versioningEnabled()),
      indexPage = info.getIndexPage,
      notFoundPage = info.getNotFoundPage,
      lifecycleRules = info.getLifecycleRules.asScala.toList,
      storageClass = info.getStorageClass,
      location = info.getLocation,
      etag = info.getEtag,
      createTime = info.getCreateTime,
      metageneration = info.getMetageneration,
      cors = info.getCors.asScala.toList,
      acl = info.getAcl.asScala.toList,
      defaultAcl = info.getDefaultAcl.asScala.toList,
      labels = info.getLabels.asScala.toMap,
      defaultKmsKeyName = info.getDefaultKmsKeyName,
      defaultEventBasedHold = Option(info.getDefaultEventBasedHold),
      retentionEffectiveTime = info.getRetentionEffectiveTime,
      retentionPolicyIsLocked = Option(info.retentionPolicyIsLocked()),
      retentionPeriod = info.getRetentionPeriod,
      iamConfiguration = info.getIamConfiguration,
      locationType = info.getLocationType,
      logging = info.getLogging
    )
  }

  def toJava(name: String, location: Location, metadata: Option[Metadata]): GoogleBucketInfo = {
    val builder = GoogleBucketInfo.newBuilder(name).setLocation(location)
    metadata.foreach(_.storageClass.foreach(builder.setStorageClass))
    metadata.foreach(_.logging.foreach(builder.setLogging))
    metadata.foreach(_.retentionPeriod.foreach(rp => builder.setRetentionPeriod(rp.toMillis)))
    metadata.foreach(_.defaultEventBasedHold.foreach(evb => builder.setDefaultEventBasedHold(evb)))

    // Booleans
    metadata.foreach(_.versioningEnabled.foreach(b => builder.setVersioningEnabled(b)))
    metadata.foreach(_.requesterPays.foreach(b => builder.setRequesterPays(b)))

    // Security and Access Control
    metadata.foreach(md => builder.setAcl(md.acl.asJava))
    metadata.foreach(md => builder.setAcl(md.acl.asJava))
    metadata.foreach(md => builder.setDefaultAcl(md.defaultAcl.asJava))
    metadata.foreach(md => builder.setCors(md.cors.asJava))
    metadata.foreach(md => builder.setLifecycleRules(md.lifecycleRules.asJava))
    metadata.foreach(_.iamConfiguration.foreach(builder.setIamConfiguration))
    metadata.foreach(_.defaultKmsKeyName.foreach(builder.setDefaultKmsKeyName))

    // Pages and Metadata
    metadata.foreach(md => builder.setLabels(md.labels.asJava))
    metadata.foreach(_.indexPage.foreach(builder.setNotFoundPage))
    metadata.foreach(_.notFoundPage.foreach(builder.setNotFoundPage))

    builder.build()
  }

  type Location = String

  object Locations {

    // Regions
    lazy val `NORTHAMERICA-NORTHEAST1`: Location = "NORTHAMERICA-NORTHEAST1"
    lazy val `US-CENTRAL1`: Location = "US-CENTRAL1"
    lazy val `US-EAST1`: Location = "US-EAST1"
    lazy val `US-EAST4`: Location = "US-EAST4"
    lazy val `US-WEST1`: Location = "US-WEST1"
    lazy val `US-WEST2`: Location = "US-WEST2"
    lazy val `US-WEST3`: Location = "US-WEST3"
    lazy val `US-WEST4`: Location = "US-WEST4"
    lazy val `SOUTHAMERICA-EAST1`: Location = "SOUTHAMERICA-EAST1"
    lazy val `EUROPE-NORTH1`: Location = "EUROPE-NORTH1"
    lazy val `EUROPE-WEST1`: Location = "EUROPE-WEST1"
    lazy val `EUROPE-WEST2`: Location = "EUROPE-WEST2"
    lazy val `EUROPE-WEST3`: Location = "EUROPE-WEST3"
    lazy val `EUROPE-WEST4`: Location = "EUROPE-WEST4"
    lazy val `EUROPE-WEST6`: Location = "EUROPE-WEST6"
    lazy val `ASIA-EAST1`: Location = "ASIA-EAST1"
    lazy val `ASIA-EAST2`: Location = "ASIA-EAST2"
    lazy val `ASIA-NORTHEAST1`: Location = "ASIA-NORTHEAST1"
    lazy val `ASIA-NORTHEAST2`: Location = "ASIA-NORTHEAST2"
    lazy val `ASIA-NORTHEAST3`: Location = "ASIA-NORTHEAST3"
    lazy val `ASIA-SOUTH1`: Location = "ASIA-SOUTH1"
    lazy val `ASIA-SOUTHEAST1`: Location = "ASIA-SOUTHEAST1"
    lazy val `AUSTRALIA-SOUTHEAST1`: Location = "AUSTRALIA-SOUTHEAST1"

    // Multi-regions
    lazy val ASIA: Location = "ASIA"
    lazy val EU: Location = "EU"
    lazy val US: Location = "US"

    // Dual-regions
    lazy val EUR4: Location = "EUR4"
    lazy val NAM4: Location = "NAM4"
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
    logging: Option[GoogleBucketInfo.Logging] = None,
    indexPage: Option[String] = None,
    notFoundPage: Option[String] = None,
    defaultKmsKeyName: Option[String] = None,
    defaultEventBasedHold: Option[Boolean] = None,
    iamConfiguration: Option[IamConfiguration] = None)
}

private[gcs] final class BucketInfo(
  name: String,
  location: String,
  owner: Acl.Entity,
  selfLink: String,
  requesterPays: Option[Boolean],
  versioningEnabled: Option[Boolean],
  indexPage: String,
  notFoundPage: String,
  lifecycleRules: List[LifecycleRule],
  storageClass: StorageClass,
  etag: String,
  createTime: Long,
  metageneration: Long,
  cors: List[Cors],
  acl: List[Acl],
  defaultAcl: List[Acl],
  labels: Map[String, String],
  defaultKmsKeyName: String,
  defaultEventBasedHold: Option[Boolean],
  retentionEffectiveTime: Long,
  retentionPolicyIsLocked: Option[Boolean],
  retentionPeriod: Long,
  iamConfiguration: IamConfiguration,
  locationType: String,
  logging: Logging,
  generatedId: String
)
