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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProviderSpec extends AnyFlatSpec with Matchers {

  s"$Provider" should "be parsed from anonymous,chain, default, environment, instance, profile, static and system" in {
    Provider.fromString("anonymous") shouldBe Provider.Anonymous
    Provider.fromString("default") shouldBe Provider.Default
    Provider.fromString("environment") shouldBe Provider.Environment
    Provider.fromString("instance") shouldBe Provider.Instance
    Provider.fromString("profile") shouldBe Provider.Profile
    Provider.fromString("static") shouldBe Provider.Static
    Provider.fromString("system") shouldBe Provider.System
    Provider.fromString("malformed") shouldBe Provider.Default
  }

  it should "not be case sensitive" in {
    Provider.fromString("Anonymous") shouldBe Provider.Anonymous
    Provider.fromString("DEFAULT") shouldBe Provider.Default
    Provider.fromString("ENVironment") shouldBe Provider.Environment
    Provider.fromString("instancE") shouldBe Provider.Instance
    Provider.fromString("Profile") shouldBe Provider.Profile
    Provider.fromString("StatiC") shouldBe Provider.Static
    Provider.fromString("SYStem") shouldBe Provider.System
  }

}
