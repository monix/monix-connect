/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
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

package monix.connect.aws.auth.configreader

import monix.connect.aws.auth.MonixAwsConf.AppConf
import monix.connect.aws.auth.{AwsCredentialsConf, HttpClientConf, MonixAwsConf, StaticCredentialsConf}
import pureconfig.ConfigReader
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

object KebabConfigReader {
  private[auth] implicit val staticCreedsConfConfigReader: ConfigReader[StaticCredentialsConf] =
    ConfigReader.forProduct3("access-key-id", "secret-access-key", "session-token")(StaticCredentialsConf(_, _, _))
  private[auth] implicit val awsCredentialsConfConfigReader: ConfigReader[AwsCredentialsConf] =
    ConfigReader.forProduct3("provider", "profile-name", "static")(AwsCredentialsConf(_, _, _))
  private[auth] implicit val credentialsProviderReader: ConfigReader[AwsCredentialsProvider] =
    ConfigReader[AwsCredentialsConf].map(_.credentialsProvider)
  private[auth] implicit val httpClientConfConfigReader: ConfigReader[HttpClientConf] = ConfigReader.forProduct8(
    "max-concurrency",
    "max-pending-connection-acquires",
    "connection-acquisition-timeout",
    "connection-max-idle-time",
    "connection-time-to-live",
    "use-idle-connection-reaper",
    "read-timeout",
    "write-timeout"
  )(HttpClientConf(_, _, _, _, _, _, _, _))
  private[auth] implicit val monixAwsConfConfigReader: ConfigReader[MonixAwsConf] =
    ConfigReader.forProduct4("region", "credentials", "endpoint", "http-client")(MonixAwsConf(_, _, _, _))
  implicit val appConfConfigReader: ConfigReader[AppConf] = ConfigReader.forProduct1("monix-aws")(AppConf(_))
}