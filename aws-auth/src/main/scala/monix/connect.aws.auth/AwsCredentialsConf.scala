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

import monix.execution.internal.InternalApi
import software.amazon.awssdk.auth.credentials.{
  AnonymousCredentialsProvider,
  AwsBasicCredentials,
  AwsCredentialsProvider,
  AwsSessionCredentials,
  DefaultCredentialsProvider,
  EnvironmentVariableCredentialsProvider,
  InstanceProfileCredentialsProvider,
  ProfileCredentialsProvider,
  StaticCredentialsProvider,
  SystemPropertyCredentialsProvider
}

@InternalApi
private[connect] final case class AwsCredentialsConf(
  provider: Provider.Type,
  profileName: Option[String],
  static: Option[StaticCredentialsConf]) {
  val credentialsProvider: AwsCredentialsProvider = {
    provider match {
      case Provider.Anonymous => AnonymousCredentialsProvider.create()
      case Provider.Default => DefaultCredentialsProvider.create()
      case Provider.Environment => EnvironmentVariableCredentialsProvider.create()
      case Provider.Instance => InstanceProfileCredentialsProvider.create()
      case Provider.Profile => {
        profileName match {
          case Some(name) => ProfileCredentialsProvider.create(name)
          case None => ProfileCredentialsProvider.create()
        }
      }
      case Provider.Static =>
        static match {
          case Some(creeds) =>
            StaticCredentialsProvider.create {
              creeds.sessionToken match {
                case None => AwsBasicCredentials.create(creeds.accessKeyId, creeds.secretAccessKey)
                case Some(token) => AwsSessionCredentials.create(creeds.accessKeyId, creeds.secretAccessKey, token)
              }
            }
          case None => DefaultCredentialsProvider.create()
        }
      case Provider.System => SystemPropertyCredentialsProvider.create()
      case _ => DefaultCredentialsProvider.create()
    }
  }
}
