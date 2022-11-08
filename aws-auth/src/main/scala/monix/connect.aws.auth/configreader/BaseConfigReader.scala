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

private[auth] trait BaseConfigReader {

  def cased(sequence: String*): String
  private[auth] implicit val staticCreedsConfConfigReader: ConfigReader[StaticCredentialsConf] =
    ConfigReader.forProduct3(cased("access", "key", "id"), cased("secret", "access", "key"), cased("session", "token"))(
      StaticCredentialsConf(_, _, _))
  private[auth] implicit val awsCredentialsConfConfigReader: ConfigReader[AwsCredentialsConf] =
    ConfigReader.forProduct3(cased("provider"), cased("profile", "name"), cased("static"))(AwsCredentialsConf(_, _, _))
  private[auth] implicit val credentialsProviderReader: ConfigReader[AwsCredentialsProvider] =
    ConfigReader[AwsCredentialsConf].map(_.credentialsProvider)
  private[auth] implicit val httpClientConfConfigReader: ConfigReader[HttpClientConf] = ConfigReader.forProduct8(
    cased("max", "concurrency"),
    cased("max", "pending", "connection", "acquires"),
    cased("connection", "acquisition", "timeout"),
    cased("connection", "max", "idle", "time"),
    cased("connection", "time", "to", "live"),
    cased("use", "idle", "connection", "reaper"),
    cased("read", "timeout"),
    cased("write", "timeout")
  )(HttpClientConf(_, _, _, _, _, _, _, _))
  private[auth] implicit val monixAwsConfConfigReader: ConfigReader[MonixAwsConf] =
    ConfigReader.forProduct4(cased("region"), cased("credentials"), cased("endpoint"), cased("http", "client"))(
      MonixAwsConf(_, _, _, _))
  private[auth] implicit val appConfConfigReader: ConfigReader[AppConf] =
    ConfigReader.forProduct1(cased("monix", "aws"))(AppConf(_))
}
