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

package monix.connect.ksqldb.models.ksql.ddl

import monix.connect.ksqldb.models.ValidationUtils
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues

class DDLInfoTest extends AnyWordSpec with Matchers with EitherValues {

  "DDLInfo" should {

    "encode to json" in {

      val testObj: DDLInfo = DDLInfo("test", "test", CommandStatus("test", "test"), 1)
      val json =
        """{"statementText":"test","commandId":"test","commandStatus":{"status":"test","message":"test"},"commandSequenceNumber":1}"""

      ValidationUtils.validateEncode[DDLInfo](testObj, json)

    }

    "decode from json" in {

      val testObjects = Seq(
        DDLInfo(
          "CREATE STREAM pageviews_home AS SELECT * FROM pageviews_original WHERE pageid='home';",
          "stream/PAGEVIEWS_HOME/create",
          CommandStatus("SUCCESS", "Stream created and running"),
          1
        ),
        DDLInfo(
          "CREATE STREAM pageviews_alice AS SELECT * FROM pageviews_original WHERE userid='alice';",
          "stream/PAGEVIEWS_ALICE/create",
          CommandStatus("SUCCESS", "Stream created and running"),
          2
        )
      )

      ValidationUtils.validateDecode[Seq[DDLInfo]](testObjects, "ksql/responses/ddl.json")

    }

  }

}
