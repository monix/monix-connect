package monix.connect.aws.auth

import software.amazon.awssdk.regions.Region
import pureconfig._
import pureconfig.generic.auto._

case class AwsClientConf(
  credentials: CredentialsConf,
  endpoint: Option[String],
  httpClient: HttpClientConf,
  region: Region) {}

object AwsClientConf {
  implicit val credentialsProviderReader: ConfigReader[Provider.Type] = ConfigReader[String].map(Provider.fromString(_))
  implicit val regionsReader: ConfigReader[Region] = ConfigReader[String].map(Region.of(_))
  val load = ConfigSource.default.loadOrThrow[AwsClientConf]
}
