package scalona.monix.connect.s3

import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider

object S3AppConfig {

  implicit val confHint: ProductHint[AppConfig] = ProductHint[AppConfig](ConfigFieldMapping(SnakeCase, SnakeCase))

  case class S3Config(endPoint: String, pathStyleAccess: Boolean, credentials: AwsCredentialsConfig, region: AwsRegionConfig) {
    val awsCredentials: AwsCredentialsProvider = credentials.provider match {
      case "anonymous" => AnonymousCredentialsProvider.create()
      case "default" => DefaultCredentialsProvider.create()
    }
  }
  case class AwsRegionConfig(provider: String, default: Option[String])

  case class AwsCredentialsConfig(provider: String, accessKeyId: Option[String], secretAccessKey: Option[String], token: Option[String]) {

  }

  case class AppConfig(s3: S3Config)
  def load(): S3Config = loadConfigOrThrow[AppConfig].s3

}
