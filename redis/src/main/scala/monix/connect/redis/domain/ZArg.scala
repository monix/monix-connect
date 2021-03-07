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

package monix.connect.redis.domain

import io.lettuce.core.ZAddArgs

object ZArgs extends Enumeration {
  //Only update elements that already exist. Never add elements.
  val XX,
    //Changed elements are new elements added and elements already existing for which the score was updated.
  // So elements specified in the command line having the same score as they had in the past are not counted.
  CH,
    // Don't update already existing elements. Always add new elements.
  NX = Value
  type ZArg = Value

  def parse(zArg: ZArg): ZAddArgs = {
    zArg match {
      case ZArgs.XX => new ZAddArgs().xx()
      case ZArgs.CH => new ZAddArgs().ch()
      case ZArgs.NX => new ZAddArgs().nx()
    }
  }
}
