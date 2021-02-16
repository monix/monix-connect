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

package monix.connect.ksqldb.models.ksql.query

import monix.connect.ksqldb.models.ValidationUtils
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import org.scalatest.EitherValues

class QueryResponseTest extends AnyWordSpec with Matchers with EitherValues {

  "QueryResponse" should {

    "encode to json" in {

      val testObj: QueryResponse = QueryResponse("test", Seq(QueryInfo("test", "test", "test")))

      val json =
        """{"statementText":"test","queries":[{"queryString":"test","sinks":"test","id":"test"}]}"""

      ValidationUtils.validateEncode[QueryResponse](testObj, json)

    }

    "decode from json" in {

      val testObj: QueryResponse =
        QueryResponse("LIST QUERIES", Seq(QueryInfo("LIST QUERIES", "test, test", "test_id")))

      ValidationUtils.validateDecode[QueryResponse](testObj, "ksql/responses/query.json")

    }

  }

}
