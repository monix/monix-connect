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

package monix.connect.aws.auth

import monix.eval.Task
import software.amazon.awssdk.regions.Region
import pureconfig._
import pureconfig.error.{ConfigReaderException, ConfigReaderFailures}
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider

import java.net.URI

/**
  * Represents the aws configuration that will be used/needed by
  * the downstream monix aws dependencies (dynamo, s3, sqs).
  *
  * ==Example==
  * {{{
  *   import monix.connect.aws.auth.MonixAwsConf
  *
  *    // given that there is an `application.conf` file
  *    // under resources folder that looks like below.
  *   /**
  *    * {
  *    *    monix-aws: {
  *    *      credentials {
  *    *        provider: "default"
  *    *      }
  *    *      region: "eu-west-1"
  *    *      endpoint: "localhost:4566"
  *    *    }
  *    * }
  *    */
  *
  *   // to then read it
  *   val monixAwsConf: Task[MonixAwsConf] = MonixAwsConf.load
  * }}}
  *
  * @param region the AWS region Anonymous
  * @param credentialsProvider the credentials provider of type [[Providers.Provider]]
  * @param endpoint optional config representing the AWS endpoint
  * @param httpClient optional http configurations like maxConcurrency,
  *                   connectionTimeToLive, connectionMaxIdleTime,
  *                   read and write timeouts, etc.
  */
case class MonixAwsConf private (
  region: Region,
  credentialsProvider: AwsCredentialsProvider,
  endpoint: Option[URI],
  httpClient: Option[HttpClientConf])

object MonixAwsConf {

  private[auth] final case class AppConf(monixAws: MonixAwsConf)

  implicit val credentialsProviderReader: ConfigReader[AwsCredentialsProvider] =
    ConfigReader[AwsCredentialsConf].map(_.credentialsProvider)
  implicit val providerReader: ConfigReader[Providers.Provider] = ConfigReader[String].map(Providers.fromString)
  implicit val regionReader: ConfigReader[Region] = ConfigReader[String].map(Region.of)
  implicit val uriReader: ConfigReader[URI] = ConfigReader[String].map(URI.create)

  /**
    * Loads the aws auth configuration from the config file with the specified naming
    * convention, being [[KebabCase]] the default one.
    *
    * It overwrites the defaults values from:
    * `https://github.com/monix/monix-connect/blob/master/aws-auth/src/main/resources/reference.conf`.
    *
    * @param namingConvention the name convention to read the data from the config file.
    *
    */
  def load(namingConvention: NamingConvention = KebabCase): Task[MonixAwsConf] = {
    implicit val hint: ProductHint[MonixAwsConf] = ProductHint(
      ConfigFieldMapping(namingConvention, namingConvention),
      useDefaultArgs = true,
      allowUnknownKeys = true)
    Task
      .fromEither[ConfigReaderFailures, AppConf] { ConfigReaderException(_) }(ConfigSource.default.load[AppConf])
      .map(_.monixAws)
  }

}
