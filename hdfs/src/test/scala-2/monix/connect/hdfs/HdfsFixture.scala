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

package monix.connect.hdfs

import org.scalacheck.Gen

trait HdfsFixture {
  val genFileName: Gen[String] = Gen.identifier
  val genChunk: Gen[Array[Byte]] = Gen.identifier.map(_.getBytes)
  val genChunks: Gen[List[Array[Byte]]] = Gen.listOfN(10, genChunk)
}
