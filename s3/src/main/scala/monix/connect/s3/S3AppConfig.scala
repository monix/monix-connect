/*
 * Copyright (c) 2014-2020 by The Monix Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.s3

import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider

object S3AppConfig {

  implicit val confHint: ProductHint[AppConfig] = ProductHint[AppConfig](ConfigFieldMapping(SnakeCase, SnakeCase))

  case class S3Config(
    endPoint: String,
    pathStyleAccess: Boolean,
    credentials: AwsCredentialsConfig,
    region: AwsRegionConfig) {
    val awsCredentials: AwsCredentialsProvider = credentials.provider match {
      case "anonymous" => AnonymousCredentialsProvider.create()
      case "default" => DefaultCredentialsProvider.create()
    }
  }
  case class AwsRegionConfig(provider: String, default: Option[String])

  case class AwsCredentialsConfig(
    provider: String,
    accessKeyId: Option[String],
    secretAccessKey: Option[String],
    token: Option[String]) {}

  case class AppConfig(s3: S3Config)
  def load(): S3Config = loadConfigOrThrow[AppConfig].s3

}
