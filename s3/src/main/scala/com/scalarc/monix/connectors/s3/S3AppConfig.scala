package com.scalarc.monix.connectors.s3

import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

object S3AppConfig {

  implicit val confHint: ProductHint[AppConfig] = ProductHint[AppConfig](ConfigFieldMapping(SnakeCase, SnakeCase))

  case class S3Config(endPoint: String)

  case class AppConfig(s3: S3Config)
  def load(): S3Config = loadConfigOrThrow[AppConfig].s3


}
