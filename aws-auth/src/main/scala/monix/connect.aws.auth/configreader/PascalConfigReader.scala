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

object PascalConfigReader {
  private[auth] implicit val staticCreedsConfConfigReader: ConfigReader[StaticCredentialsConf] =
    ConfigReader.forProduct3("AccessKeyId", "SecretAccessKey", "SessionToken")(StaticCredentialsConf)
  private[auth] implicit val awsCredentialsConfConfigReader: ConfigReader[AwsCredentialsConf] =
    ConfigReader.forProduct3("Provider", "ProfileName", "Static")(AwsCredentialsConf)
  private[auth] implicit val credentialsProviderReader: ConfigReader[AwsCredentialsProvider] =
    ConfigReader[AwsCredentialsConf].map(_.credentialsProvider)
  private[auth] implicit val httpClientConfConfigReader: ConfigReader[HttpClientConf] = ConfigReader.forProduct8(
    "MaxConcurrency",
    "MaxPendingConnectionAcquires",
    "ConnectionAcquisitionTimeout",
    "ConnectionMaxIdleTime",
    "ConnectionTimeToLive",
    "UseIdleConnectionReaper",
    "ReadTimeout",
    "WriteTimeout"
  )(HttpClientConf)
  private[auth] implicit val monixAwsConfConfigReader: ConfigReader[MonixAwsConf] =
    ConfigReader.forProduct4("Region", "Credentials", "Endpoint", "HttpClient")(MonixAwsConf(_, _, _, _))
  implicit val appConfConfigReader = ConfigReader.forProduct1("MonixAws")(AppConf)
}
