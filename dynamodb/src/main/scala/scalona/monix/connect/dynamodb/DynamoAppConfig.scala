package scalona.monix.connect.dynamodb

import pureconfig._
import pureconfig.generic.ProductHint
import com.amazonaws.regions.Regions.DEFAULT_REGION
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  AwsCredentialsProvider,
  DefaultCredentialsProvider,
  StaticCredentialsProvider
}
import pureconfig.generic.auto._
object DynamoAppConfig {

  implicit val confHint: ProductHint[AppConfig] = ProductHint[AppConfig](ConfigFieldMapping(SnakeCase, SnakeCase))

  case class DynamoDbConfig(
    endPoint: String,
    pathStyleAccess: Boolean,
    credentials: AwsCredentialsConfig,
    region: AwsRegionConfig) {
    val awsCredProvider: AwsCredentialsProvider = credentials.provider match {
      case "anonymous" => StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
      case "default" => DefaultCredentialsProvider.create()
    }

    val awsRegion: String = region.provider match {
      case "static" => region.default.getOrElse(DEFAULT_REGION.getName)
      case "default" => ""
    }
  }
  case class AwsRegionConfig(provider: String, default: Option[String])

  case class AwsCredentialsConfig(
    provider: String,
    accessKeyId: Option[String],
    secretAccessKey: Option[String],
    token: Option[String]) {}

  case class AppConfig(dynamoDb: DynamoDbConfig)
  def load(): DynamoDbConfig = loadConfigOrThrow[AppConfig].dynamoDb

}
