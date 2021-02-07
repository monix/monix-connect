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

package monix.connect.ksqldb.models.debug.describe

import monix.connect.ksqldb.models.ValidationUtils
import monix.connect.ksqldb.models.debug.shared.{FieldInfo, SchemaInfo}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SourceDescriptionTest extends AnyWordSpec with Matchers {

  "SourceDescription" should {

    "decode from json" in {

      val sourceDescription = SourceDescription(
        "test",
        List("test", "test"),
        List("test", "test"),
        List(FieldInfo("test", SchemaInfo("STRING", None, None))),
        "STREAM",
        "time",
        "time",
        "AVRO",
        "test",
        extended = false,
        None,
        None,
        None,
        None
      )

      val testObject = DescribeResult(sourceDescription)

      ValidationUtils
        .validateDecode[DescribeResult](testObject, "debug/describe/describe_result.json")

    }

  }

}
