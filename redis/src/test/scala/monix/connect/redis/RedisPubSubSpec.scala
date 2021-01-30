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

import java.lang

import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.{verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class RedisPubSubSpec
  extends AnyFlatSpec with Matchers with IdiomaticMockito with BeforeAndAfterEach with BeforeAndAfterAll
  with RedisFixture {

  implicit val connection: StatefulRedisConnection[String, Int] = mock[StatefulRedisConnection[String, Int]]

  override def beforeAll(): Unit = {
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    super.beforeAll()
  }

  override def beforeEach() = {
    reset(reactiveRedisCommands)
    reset(reactiveRedisCommands)
  }

  /*
  s"${RedisPubSub} " should " implement RedisPubSub trait" in {
    RedisPubSub shouldBe a[RedisPubSub]
  }

  it should "implement publish" in {
    //given
    val channel: K = genRedisKey.sample.get
    val message: V = genRedisValue.sample.get
    when(reactiveRedisCommands.publish(channel, message)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisPubSub.publish(channel, message)

    //then
    verify(reactiveRedisCommands).publish(channel, message)
  }

  it should "implement pubsubChannels" in { //todo
    //given
    when(reactiveRedisCommands.pubsubChannels()).thenReturn(mockFlux[String])

    //when
    //val ob = RedisPubSub.pubsubChannels()(connection)

    //then
    //ob shouldBe a[Observable[String]]
    //verify(reactiveRedisCommands).pubsubChannels()
  }

  it should "implement pubsubNumsub" in {
    //given
    val channels: List[String] = genRedisKeys.sample.get
    when(reactiveRedisCommands.pubsubNumsub(channels: _*))
      .thenReturn(mockMono[java.util.Map[String, java.lang.Long]])

    //when
    val _: Task[Map[String, lang.Long]] = RedisPubSub.pubsubNumsub(channels: _*)

    //then
    verify(reactiveRedisCommands).pubsubNumsub(channels: _*)
  }

  it should "implement pubsubNumpat" in {
    //given
    when(reactiveRedisCommands.pubsubNumpat()).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisPubSub.pubsubNumpat()

    //then
    verify(reactiveRedisCommands).pubsubNumpat()
  }

  it should "implement echo" in {
    //given
    val message: V = genRedisValue.sample.get
    when(reactiveRedisCommands.echo(message)).thenReturn(mockMono[Int])

    //when
    val _: Task[V] = RedisPubSub.echo(message)

    //then
    verify(reactiveRedisCommands).echo(message)
  }

  //todo
  /*  it should "implement role" in {
    //given
    when(reactiveRedisCommands.role()).thenReturn(MockFlux[_])

    //when
    val _ = RedisPubSub.role()

    //then
    verify(reactiveRedisCommands).role()
  }*/

  it should "implement ping" in {
    //given
    when(reactiveRedisCommands.ping()).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisPubSub.ping()

    //then
    verify(reactiveRedisCommands).ping()
  }

  it should "implement readOnly" in {
    //given
    val channel: String = genRedisKey.sample.get
    val message: Int = genRedisValue.sample.get
    when(reactiveRedisCommands.readOnly()).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisPubSub.readOnly()

    //then
    verify(reactiveRedisCommands).readOnly()
  }

  it should "implement readWrite" in {
    //given
    when(reactiveRedisCommands.readWrite()).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisPubSub.readWrite()

    //then
    verify(reactiveRedisCommands).readWrite()
  }

  it should "implement quit" in {
    //given
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    when(reactiveRedisCommands.quit()).thenReturn(mockMono[String])

    //when
    val _: Task[String] = RedisPubSub.quit()

    //then
    verify(reactiveRedisCommands).quit()
  }

  it should "implement waitForReplication" in {
    //given
    val replicas: Int = genLong.sample.get.toInt
    val timeout: Long = genLong.sample.get
    when(reactiveRedisCommands.waitForReplication(replicas, timeout)).thenReturn(mockMono[java.lang.Long])

    //when
    val _: Task[Long] = RedisPubSub.waitForReplication(replicas, timeout)

    //then
    verify(reactiveRedisCommands).waitForReplication(replicas, timeout)
  }
  //dispatch not supported
*/
}
