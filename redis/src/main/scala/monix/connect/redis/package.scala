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

package monix.connect

import io.lettuce.core.KeyValue
import monix.connect.redis.client.Codec

import scala.util.Try

package object redis {

  private[redis] def kvToTuple[K, V](kv: KeyValue[K, V]): (K, Option[V]) = {
    (kv.getKey, Try(kv.getValue).toOption)
  }

  implicit val intUtfCodec: Codec[Int, String] = Codec.utf(_.toString, str => Try(str.toInt).getOrElse(0))
  implicit val doubleUtfCodec: Codec[Double, String] = Codec.utf(_.toString, str => Try(str.toDouble).getOrElse(0.0))
  implicit val floatUtfCodec: Codec[Float, String] = Codec.utf(_.toString, str => Try(str.toFloat).getOrElse(0L))
  implicit val bigIntUtfCodec: Codec[BigInt, String] = Codec.utf(_.toString, str => Try(BigInt.apply(str)).getOrElse(0))
  implicit val bigDecimalUtfCodec: Codec[BigDecimal, String] =
    Codec.utf(_.toString, str => Try(BigDecimal.apply(str)).getOrElse(0.0))

}
