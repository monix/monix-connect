/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

package monix.connect.aws.auth

import monix.execution.internal.InternalApi
import pureconfig.ConfigReader.Result
import software.amazon.awssdk.regions.Region
import pureconfig._
import pureconfig.generic.auto._
import software.amazon.awssdk.auth.credentials.{AnonymousCredentialsProvider, AwsBasicCredentials, AwsCredentialsProvider, AwsSessionCredentials, DefaultCredentialsProvider, EnvironmentVariableCredentialsProvider, InstanceProfileCredentialsProvider, ProfileCredentialsProvider, StaticCredentialsProvider, SystemPropertyCredentialsProvider}

@InternalApi
private[connect] final case class AwsClientConf(
  credentials: AwsCredentialsProvider,
  endpoint: Option[String],
  httpClient: Option[HttpClientConf],
  region: Region) {}

@InternalApi
private[connect] object AwsClientConf {
  implicit val awsCredentialsProviderReader: ConfigReader[AwsCredentialsProvider] =
    ConfigReader[AwsCredentialsConf].map{ credentialsConf => credentialsConf.credentialsProvider}
  implicit val credentialsProviderReader: ConfigReader[Provider.Type] = ConfigReader[String].map(Provider.fromString(_))
  implicit val regionsReader: ConfigReader[Region] = ConfigReader[String].map(Region.of(_))
  val load: Result[AwsClientConf] = ConfigSource.default.load[AwsClientConf]
  val loadOrThrow = ConfigSource.default.loadOrThrow[AwsClientConf]

}
