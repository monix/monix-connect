package scalona.monix.connectors.redis

import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

object S3AppConfig {

  implicit val confHint: ProductHint[AppConfig] = ProductHint[AppConfig](ConfigFieldMapping(SnakeCase, SnakeCase))

  case class RedisConfig(endPoint: String, pathStyleAccess: Boolean, credentials: AwsCredentialsConfig, region: AwsRegionConfig)
  case class AwsRegionConfig(provider: String, default: Option[String])

  case class AwsCredentialsConfig(provider: String, accessKeyId: Option[String], secretAccessKey: Option[String], token: Option[String]) {

  }

  case class AppConfig(s3: RedisConfig)
  def load(): RedisConfig = loadConfigOrThrow[AppConfig].s3

} val redisClient: RedisClient = RedisClient.create("redis://localhost:6379/0")
val connection: StatefulRedisConnection[String, String] = redisClient.connect()
//#create-redis-connection
