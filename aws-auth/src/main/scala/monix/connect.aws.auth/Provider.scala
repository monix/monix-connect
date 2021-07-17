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

@InternalApi
private[connect] object Providers extends Enumeration {

  type Provider = Value
  val Anonymous, Default, Environment, Instance, System, Profile, Static = Value

  def fromString(str: String): Providers.Provider = {
    str.toLowerCase match {
      case "anonymous" => Anonymous
      case "default" => Default
      case "environment" => Environment
      case "instance" => Instance
      case "profile" => Profile
      case "static" => Static
      case "system" => System
      case _ => Default
    }
  }
}
