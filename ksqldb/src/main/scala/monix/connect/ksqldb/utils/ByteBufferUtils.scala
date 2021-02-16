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

import java.nio.ByteBuffer

import scala.util.control.Breaks

/**
  * Utils for working with Byte Buffer
  *
  * @author Andrey Romanov
  */
object ByteBufferUtils {

  /**
    * Method for searching a symbol in buffer
    * @param search symbol for search in byte data
    * @param data byte data
    * @return info, if it exists, or not
    * @author Andrey Romanov and @quelgar. Original source: https://gist.github.com/quelgar/3e8aa15d3211f4c791acceeeef6beeef
    */
  def findInBuffer(search: ByteBuffer, data: ByteBuffer): Boolean = {
    val initDataPos: Int = data.position
    while (data.hasRemaining) {
      if (data.remaining < search.remaining) {
        return false
      }
      val dataMatch = data.asReadOnlyBuffer
      var matched = true
      val myBreaks = new Breaks
      myBreaks.breakable {
        while (search.hasRemaining && dataMatch.hasRemaining) {
          if (search.get() != dataMatch.get()) {
            matched = false
            myBreaks.break()
          }
        }
      }
      search.position(0)
      if (matched) {
        return true
      }
      data.get()
    }
    data.position(initDataPos)
    false
  }

}
