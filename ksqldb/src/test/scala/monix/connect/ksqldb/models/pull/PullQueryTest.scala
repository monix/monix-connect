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

package monix.connect.ksqldb.models.pull

import monix.connect.ksqldb.models.{ClientError, ValidationUtils}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JArray
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PullQueryTest extends AnyWordSpec with Matchers {

  "PullQuery model" should {

    "decode request from json" in {

      val expectedObj = PullRequest("select * from foo", Map("prop1" -> "val1", "prop2" -> "val2"))

      ValidationUtils.validateDecode[PullRequest](expectedObj, "pull/request.json")

    }

    "decode schema from json" in {

      val expectedObj = ResponseSchema(
        Some("xyz123"),
        List("col", "col2", "col3"),
        List("BIGINT", "STRING", "BOOLEAN")
      )

      ValidationUtils.validateDecode[ResponseSchema](expectedObj, "pull/schema-response.json")

    }

    "decode data from json" in {

      val jsonString: String = ValidationUtils.getJSONData("models/pull/data-response.json")

      val decodeResult: Either[ClientError, PullResponse] = convert(jsonString)

      assert(decodeResult.isRight)

      val pullResponse: PullResponse = decodeResult.toOption.get

      pullResponse.isSchema shouldBe false
      pullResponse.data.isEmpty shouldBe false

      val dataArray: JArray = pullResponse.data.get

      implicit val format: DefaultFormats = DefaultFormats

      dataArray.arr(0).extract[Int] shouldBe 765
      dataArray.arr(1).extract[String] shouldBe "whatever"
      dataArray.arr(2).extract[Boolean] shouldBe false

    }

  }

}
