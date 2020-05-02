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

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.{KeyValue, MapScanCursor, RedisFuture, ScoredValue}
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.api.reactive.RedisReactiveCommands
import org.scalacheck.Gen
import reactor.core.publisher.Flux
import org.mockito.MockitoSugar.mock
import io.lettuce.core.Range

trait RedisFixture {

  type K = String
  type V = Int
  val genLong: Gen[Long] = Gen.chooseNum(1, 100000).map(_.toLong)
  val genDouble: Gen[Double] = Gen.chooseNum[Double](1, 100000)
  val genInt: Gen[Int] = Gen.chooseNum[Int](1, 100000)
  val genBool: Gen[Boolean] = Gen.oneOf(Seq(true, false))
  val genRedisKey: Gen[String] = Gen.alphaStr
  val genRedisKeys: Gen[List[String]] = for {
    n    <- Gen.chooseNum(1, 10)
    keys <- Gen.listOfN(n, Gen.alphaStr)
  } yield keys
  val genBytes: Gen[Array[Byte]] = Gen.alphaLowerStr.map(_.getBytes)

  val genRedisValue: Gen[V] = Gen.choose(0, 10000)
  val genRedisValues: Gen[List[V]] = for {
    n      <- Gen.chooseNum(2, 10)
    values <- Gen.listOfN(n, Gen.choose(0, 10000))
  } yield values
  val unboundedRange = Range.unbounded()
  val genScoredValue: Gen[ScoredValue[V]] = for {
    score <- genDouble
    value <- genRedisValue
  } yield ScoredValue.just(score, value)
  val genScoredValues: Gen[List[ScoredValue[V]]] = Gen.listOf(genScoredValue)
  val genKV: Gen[(K, V)] = for {
    k <- Gen.alphaLowerStr
    v <- Gen.chooseNum(1, 10000)
  } yield (k, v)
  val genKvMap: Gen[Map[K, V]] = Gen.mapOfN(10, genKV)
  //mocks
  def MockRedisConnection[K, V]: StatefulRedisConnection[K, V] = mock[StatefulRedisConnection[K, V]]
  def MockRedisFuture[V]: RedisFuture[V] = mock[RedisFuture[V]]
  def MockFlux[T]: Flux[T] = mock[Flux[T]]
  val asyncRedisCommands = mock[RedisAsyncCommands[String, Int]]
  val reactiveRedisCommands = mock[RedisReactiveCommands[String, Int]]
}
