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

package monix.connect.ksqldb.models.ksql.table

import monix.connect.ksqldb.models.ValidationUtils
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.scalatest.EitherValues

class TableResponseTest extends AnyWordSpec with Matchers with EitherValues {

  "TableResponse" should {

    "encode to json" in {

      val testObj: TableResponse =
        TableResponse("test", Seq(TableInfo("test", "test", "test", "TABLE", true)))

      val json =
        """{"statementText":"test","tables":[{"name":"test","topic":"test","format":"test","type":"TABLE","isWindowed":true}]}"""

      ValidationUtils.validateEncode[TableResponse](testObj, json)

    }

    "decode from json" in {

      val testObj: TableResponse =
        TableResponse(
          "LIST TABLES",
          Seq(TableInfo("test_table", "test_topic", "JSON", "TABLE", true))
        )

      ValidationUtils.validateDecode[TableResponse](testObj, "ksql/responses/table.json")

    }

  }

}
