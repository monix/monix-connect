package com.scalarc.monix.connectors.s3

import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

object S3Config {

  implicit val confHint: ProductHint[S3Config] = ProductHint[S3Config](ConfigFieldMapping(CamelCase, KebabCase))

  case class S3Config(host: String, port: Int, endPoint: String)

  def load(): S3Config = loadConfigOrThrow[S3Config]


}
