package cloriko.monix.connect.s3

import java.net.URI

import S3AppConfig.S3Config
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.Region.AWS_GLOBAL

object AsyncClient {
  def apply(): S3AsyncClient = {
    val s3Config: S3Config = S3AppConfig.load()
    S3AsyncClient.builder()
      .credentialsProvider(s3Config.awsCredentials)
      .region(AWS_GLOBAL)
      .endpointOverride(URI.create(s3Config.endPoint))
      .build
  }
}
