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

package monix.connect.ksqldb.utils

import java.net.URL
import java.nio.file.{Path, Paths}

import monix.eval.Task
import monix.nio.text.UTF8Codec._
import monix.nio.file
import monix.execution.Scheduler.Implicits.global
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class FramingTest extends AnyWordSpec with Matchers {

  "Framing" should {

    "be processed for input with multiple lines" in {

      val resourceURL: URL = getClass.getClassLoader.getResource("framing/test.txt")
      val testFilePath: Path = Paths.get(resourceURL.toURI)

      val lineTerm = Array('\n'.toByte)

      val expectedList: List[String] = List("Test", "Lines", "For", "Framing", "Execution")

      val lines: Task[List[String]] = file
        .readAsync(testFilePath, 500)
        .pipeThrough(Framing(lineTerm, 200))
        .pipeThrough(utf8Decode)
        .toListL

      lines.runSyncUnsafe() shouldBe expectedList

    }

  }

}
