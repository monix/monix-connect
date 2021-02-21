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

package monix.connect.redis

import io.lettuce.core.ScoredValue

import java.util.Optional

class VScore[V](val score: Double, val value: Option[V]) {

  def mapScore(f: Double => Double): VScore[V] =  VScore(f(score), value)

  def flatMap(f: Option[V] => Option[V]) = new VScore(score, f(value))

  private[redis] def toScoredValue: ScoredValue[V] = {
    val optional: Optional[V] = value.map(a => Optional.of(a)).getOrElse(Optional.empty())
    ScoredValue.from(score, optional)
  }
}

object VScore {

  def apply[V](score: Double, value: Option[V]) =  new VScore(score, value)

  def from[V](scoredValue: ScoredValue[V]) = new VScore[V](scoredValue.getScore, Option(scoredValue.getValue))

  def empty[V] = new VScore(0, Option.empty[V])

}
