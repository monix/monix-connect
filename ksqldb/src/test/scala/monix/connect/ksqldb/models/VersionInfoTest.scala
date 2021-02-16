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

package monix.connect.ksqldb.models

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.scalatest.EitherValues

class VersionInfoTest extends AnyWordSpec with Matchers with EitherValues {

  "VersionInfo" should {

    "encode to json" in {

      val childObj: VersionInfo = VersionInfo("0.0.1", "test", "test")
      val testObj: KSQLVersionResponse = KSQLVersionResponse(childObj)

      ValidationUtils.validateEncode[KSQLVersionResponse](
        testObj,
        """{"KsqlServerInfo":{"version":"0.0.1","kafkaClusterId":"test","ksqlServiceId":"test"}}"""
      )

    }

    "decode from json" in {

      val childObj: VersionInfo = VersionInfo("5.1.2", "j3tOi6E_RtO_TMH3gBmK7A", "default_")
      val testObj: KSQLVersionResponse = KSQLVersionResponse(childObj)

      ValidationUtils.validateDecode[KSQLVersionResponse](testObj, "version_info.json")

    }

  }

}
