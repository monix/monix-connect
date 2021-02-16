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

package monix.connect.ksqldb.models.terminate

import monix.connect.ksqldb.models.ValidationUtils
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TopicsForTerminateTest extends AnyWordSpec with Matchers with EitherValues {

  "TopicsForTerminate" should {

    "encode to json" in {

      val testObj = TopicsForTerminate(List("FOO", "bar.*"))
      val jsonString = """{"deleteTopicList":["FOO","bar.*"]}""".stripMargin

      ValidationUtils.validateEncode[TopicsForTerminate](testObj, jsonString)

    }

  }

}
