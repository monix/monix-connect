package scalarc.monix.connectors.s3

import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions.DEFAULT_REGION
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import S3AppConfig.S3Config

object S3Client {
  def apply(): AmazonS3 = {
    val s3Config: S3Config = S3AppConfig.load()
    AmazonS3ClientBuilder
      .standard()
      .withPathStyleAccessEnabled(s3Config.pathStyleAccess)
      .withEndpointConfiguration(new EndpointConfiguration(s3Config.endPoint, DEFAULT_REGION.getName))
      .withCredentials(s3Config.awsCredentials)
      .build
  }
}
