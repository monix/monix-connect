package io.monix.connect.gcs.configuration

import com.google.cloud.storage.BucketInfo.{IamConfiguration, LifecycleRule}
import com.google.cloud.storage.{Acl, BucketInfo, Cors, StorageClass}
import io.monix.connect.gcs.configuration.GoogleStorageBucketConfig.Location

import scala.concurrent.duration.FiniteDuration

final case class GoogleStorageBucketConfig(name: String,
                                           logging: Option[BucketInfo.Logging],
                                           requesterPays: Option[Boolean] = None,
                                           versioningEnabled: Option[Boolean] = None,
                                           storageClass: Option[StorageClass] = None,
                                           retentionPeriod: Option[FiniteDuration] = None,
                                           acl: List[Acl] = List.empty[Acl],
                                           cors: List[Cors] = List.empty[Cors],
                                           defaultAcl: List[Acl] = List.empty[Acl],
                                           lifecycleRules: List[LifecycleRule] = List.empty[LifecycleRule],
                                           labels: Map[String, String] = Map.empty[String, String],
                                           location: Option[Location] = None,
                                           indexPage: Option[String] = None,
                                           notFoundPage: Option[String] = None,
                                           defaultKmsKeyName: Option[String] = None,
                                           defaultEventBasedHold: Option[Boolean] = None,
                                           iamConfiguration: Option[IamConfiguration] = None)

object GoogleStorageBucketConfig {

  type Location = String
  object Locations {

    // Regions
    lazy val `NORTHAMERICA-NORTHEAST1`: Location = "NORTHAMERICA-NORTHEAST1"
    lazy val `US-CENTRAL1`: Location             = "US-CENTRAL1"
    lazy val `US-EAST1`: Location                = "US-EAST1"
    lazy val `US-EAST4`: Location                = "US-EAST4"
    lazy val `US-WEST1`: Location                = "US-WEST1"
    lazy val `US-WEST2`: Location                = "US-WEST2"
    lazy val `US-WEST3`: Location                = "US-WEST3"
    lazy val `US-WEST4`: Location                = "US-WEST4"
    lazy val `SOUTHAMERICA-EAST1`: Location      = "SOUTHAMERICA-EAST1"
    lazy val `EUROPE-NORTH1`: Location           = "EUROPE-NORTH1"
    lazy val `EUROPE-WEST1`: Location            = "EUROPE-WEST1"
    lazy val `EUROPE-WEST2`: Location            = "EUROPE-WEST2"
    lazy val `EUROPE-WEST3`: Location            = "EUROPE-WEST3"
    lazy val `EUROPE-WEST4`: Location            = "EUROPE-WEST4"
    lazy val `EUROPE-WEST6`: Location            = "EUROPE-WEST6"
    lazy val `ASIA-EAST1`: Location              = "ASIA-EAST1"
    lazy val `ASIA-EAST2`: Location              = "ASIA-EAST2"
    lazy val `ASIA-NORTHEAST1`: Location         = "ASIA-NORTHEAST1"
    lazy val `ASIA-NORTHEAST2`: Location         = "ASIA-NORTHEAST2"
    lazy val `ASIA-NORTHEAST3`: Location         = "ASIA-NORTHEAST3"
    lazy val `ASIA-SOUTH1`: Location             = "ASIA-SOUTH1"
    lazy val `ASIA-SOUTHEAST1`: Location         = "ASIA-SOUTHEAST1"
    lazy val `AUSTRALIA-SOUTHEAST1`: Location    = "AUSTRALIA-SOUTHEAST1"

    // Multi-regions
    lazy val ASIA: Location = "ASIA"
    lazy val EU: Location   = "EU"
    lazy val US: Location   = "US"

    // Dual-regions
    lazy val EUR4: Location = "EUR4"
    lazy val NAM4: Location = "NAM4"
  }
}