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

import java.net.URI

import S3AppConfig.S3Config
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.Region.AWS_GLOBAL

object AsyncClient {
  def apply(): S3AsyncClient = {
    val s3Config: S3Config = S3AppConfig.load()
    S3AsyncClient
      .builder()
      .credentialsProvider(s3Config.awsCredentials)
      .region(AWS_GLOBAL)
      .endpointOverride(URI.create(s3Config.endPoint))
      .build
  }
}
