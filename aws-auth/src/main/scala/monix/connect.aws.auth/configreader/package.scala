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

import pureconfig.ConfigReader
import software.amazon.awssdk.regions.Region

import java.net.URI

package object configreader {

  implicit val providerReader: ConfigReader[Providers.Provider] = ConfigReader[String].map(Providers.fromString)
  implicit val regionReader: ConfigReader[Region] = ConfigReader[String].map(Region.of)
  implicit val uriReader: ConfigReader[URI] = ConfigReader[String].map(URI.create)

}
