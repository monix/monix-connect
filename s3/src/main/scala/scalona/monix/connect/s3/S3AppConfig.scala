package scalona.monix.connect.s3

import pureconfig._
import pureconfig.generic.ProductHint
import com.amazonaws.regions.Regions.DEFAULT_REGION
import com.amazonaws.auth.{AWSCredentialsProvider, AWSStaticCredentialsProvider, AnonymousAWSCredentials, DefaultAWSCredentialsProviderChain}
import pureconfig.generic.auto._

object S3AppConfig {

  implicit val confHint: ProductHint[AppConfig] = ProductHint[AppConfig](ConfigFieldMapping(SnakeCase, SnakeCase))

  case class S3Config(endPoint: String, pathStyleAccess: Boolean, credentials: AwsCredentialsConfig, region: AwsRegionConfig) {
    val awsCredentials: AWSCredentialsProvider = credentials.provider match {
      case "anonymous" => new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())
      case "default" => new DefaultAWSCredentialsProviderChain()
    }
    val awsRegion: String = region.provider match {
      case "static" => region.default.getOrElse(DEFAULT_REGION.getName)
      case "default" => ""
    }
  }
  case class AwsRegionConfig(provider: String, default: Option[String])

  case class AwsCredentialsConfig(provider: String, accessKeyId: Option[String], secretAccessKey: Option[String], token: Option[String]) {

  }

  case class AppConfig(s3: S3Config)
  def load(): S3Config = loadConfigOrThrow[AppConfig].s3

}
