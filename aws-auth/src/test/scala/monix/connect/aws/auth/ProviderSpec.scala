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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class ProviderSpec extends AnyFlatSpec {

  s"$Providers" should "be parsed from anonymous,chain, default, environment, instance, profile, static and system" in {
    Providers.fromString("anonymous") shouldBe Providers.Anonymous
    Providers.fromString("default") shouldBe Providers.Default
    Providers.fromString("environment") shouldBe Providers.Environment
    Providers.fromString("instance") shouldBe Providers.Instance
    Providers.fromString("profile") shouldBe Providers.Profile
    Providers.fromString("static") shouldBe Providers.Static
    Providers.fromString("system") shouldBe Providers.System
    Providers.fromString("malformed") shouldBe Providers.Default
  }

  it should "not be case sensitive" in {
    Providers.fromString("Anonymous") shouldBe Providers.Anonymous
    Providers.fromString("DEFAULT") shouldBe Providers.Default
    Providers.fromString("ENVironment") shouldBe Providers.Environment
    Providers.fromString("instancE") shouldBe Providers.Instance
    Providers.fromString("Profile") shouldBe Providers.Profile
    Providers.fromString("StatiC") shouldBe Providers.Static
    Providers.fromString("SYStem") shouldBe Providers.System
  }

}
