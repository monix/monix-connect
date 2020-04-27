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

package monix.connect.redis

import io.lettuce.core.{KeyValue, RedisFuture}
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.scalacheck.Gen
import reactor.core.publisher.Flux
import org.mockito.MockitoSugar.mock

trait RedisFixture {
  val genRedisKey: () => String = () => Gen.alphaStr.sample.get
  val genRedisKeys: Int => List[String] = n => Gen.listOfN(n, Gen.alphaStr).sample.get
  val genRedisValue: () => Int = () => Gen.choose(0, 10000).sample.get
  val genRedisValues: Int => List[Int] = n => Gen.listOfN(n, Gen.choose(0, 10000)).sample.get

  val asyncRedisCommands = mock[RedisAsyncCommands[String, Int]]
  val reactiveRedisCommands = mock[RedisReactiveCommands[String, Int]]
  val boolRedisFuture = mock[RedisFuture[java.lang.Boolean]]
  val longRedisFuture = mock[RedisFuture[java.lang.Long]]
  val strRedisFuture = mock[RedisFuture[String]]
  val strListRedisFuture = mock[RedisFuture[java.util.List[String]]]
  val kVFluxRedisFuture = mock[Flux[KeyValue[String, Int]]]
  val mapRedisFuture = mock[RedisFuture[java.util.Map[String, Int]]]
  val vRedisFuture = mock[RedisFuture[Int]]
  val vListRedisFuture = mock[RedisFuture[java.util.List[Int]]]

}
