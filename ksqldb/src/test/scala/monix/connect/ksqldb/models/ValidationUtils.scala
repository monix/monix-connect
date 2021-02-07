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

import org.scalatest.Assertion
import tethys._
import tethys.jackson._
import org.scalatest.matchers.should.Matchers
import tethys.JsonWriter
import tethys.readers.ReaderError

import scala.io.{BufferedSource, Source}

object ValidationUtils extends Matchers {

  def getJSONData(filePath: String): String = {

    val fileData: BufferedSource = Source.fromResource(filePath)
    val fileString: String = fileData.mkString
    fileData.close()

    fileString

  }

  def validateEncode[A: JsonWriter](obj: A, jsonString: String): Assertion = {

    val resultString: String = obj.asJson

    resultString shouldBe jsonString

  }

  def validateDecode[A: JsonReader](expectedObj: A, filePath: String): Assertion = {

    val fileString: String = getJSONData(s"models/${filePath}")

    val fileObj: Either[ReaderError, A] = fileString.jsonAs[A]

    fileObj should be(Symbol("right"))

    fileObj.toOption.get shouldBe expectedObj

  }

}
