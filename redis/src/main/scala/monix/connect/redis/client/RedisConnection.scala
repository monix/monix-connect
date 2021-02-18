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

package monix.connect.redis.client

import cats.effect.Resource
import monix.eval.Task

trait RedisConnection {

  /**
    *
    */
  def connectUtf: Resource[Task, RedisCmd[String, String]]

  /**
    *
    * @param keyCodec
    * @param valueCodec
    * @tparam K
    * @tparam V
    * @return
    */
  def connectUtf[K, V](
    implicit keyCodec: Codec[K, String],
    valueCodec: Codec[V, String]): Resource[Task, RedisCmd[K, V]]

  /**
    *
    * @param keyCodec
    * @param valueCodec
    * @tparam K
    * @tparam V
    * @return
    */
  def connectByteArray[K, V](
    implicit keyCodec: Codec[K, Array[Byte]],
    valueCodec: Codec[V, Array[Byte]]): Resource[Task, RedisCmd[K, V]]

}
