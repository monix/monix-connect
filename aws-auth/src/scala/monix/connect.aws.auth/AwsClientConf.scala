package monix.connect.aws.auth

import monix.execution.internal.InternalApi
import pureconfig.ConfigReader.Result
import software.amazon.awssdk.regions.Region
import pureconfig._
import pureconfig.generic.auto._

@InternalApi
private[connect] case class AwsClientConf(
                          credentials: AwsCredentialsProviderConf,
                          endpoint: Option[String],
                          httpClient: Option[HttpClientConf],
                          region: Region) {}

@InternalApi
private[connect] object AwsClientConf {
  implicit val credentialsProviderReader: ConfigReader[Provider.Type] = ConfigReader[String].map(Provider.fromString(_))
  implicit val regionsReader: ConfigReader[Region] = ConfigReader[String].map(Region.of(_))
  val load: Result[AwsClientConf] = ConfigSource.default.load[AwsClientConf]
}
