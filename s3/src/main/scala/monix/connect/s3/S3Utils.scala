/*
 * Copyright (c) 2014-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
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

package monix.connect.s3

object S3Utils {

  private val awsPartSizeLimit = 5 * 1024 * 1024

  def divideChunk(byteBuffer: List[Byte]): List[List[Byte]] = {
    val bufferSize = byteBuffer.size
    if (bufferSize > awsPartSizeLimit) {
      val (l1: List[Byte], l2: List[Byte]) = byteBuffer.splitAt(bufferSize / 2)
      (divideChunk(l1) :: divideChunk(l2) :: Nil).flatten
    } else List(byteBuffer)
  }
}
