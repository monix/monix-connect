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

import io.lettuce.core.ScoredValue

import java.util.Optional
import scala.util.Try

case class VScore[V] private[redis] (val value: Option[V], val score: Double) {

  def mapScore(f: Double => Double): VScore[V] = VScore(value, f(score))

  def flatMap(f: Option[V] => Option[V]) = new VScore(f(value), score)

  private[redis] def toScoredValue: ScoredValue[V] = {
    val optional: Optional[V] = value.map(a => Optional.of(a)).getOrElse(Optional.empty())
    ScoredValue.from(score, optional)
  }
}

object VScore {

  def apply[V](value: V, score: Double) = new VScore(Some(value), score)

  def from[V](scoredValue: ScoredValue[V]) = new VScore[V](Try(scoredValue.getValue).toOption, scoredValue.getScore)

  def empty[V] = new VScore(Option.empty[V], 0)

}
