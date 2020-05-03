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
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(connection.reactive()).thenAnswer(reactiveRedisCommands)
    super.beforeAll()
  }

  override def beforeEach() = {
    reset(asyncRedisCommands)
    reset(reactiveRedisCommands)
  }

  s"${RedisPubSub} " should " implement RedisPubSub trait" in {
    RedisPubSub shouldBe a[RedisPubSub]
  }

  it should "implement publish" in {
    //given
    val channel: K = genRedisKey.sample.get
    val message: V = genRedisValue.sample.get
    when(asyncRedisCommands.publish(channel, message)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisPubSub.publish(channel, message)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).publish(channel, message)
  }

  it should "implement pubsubChannels" in { //todo
    //given
    when(reactiveRedisCommands.pubsubChannels()).thenReturn(MockFlux[String])

    //when
    //val ob = RedisPubSub.pubsubChannels()(connection)

    //then
    //ob shouldBe a[Observable[String]]
    //verify(reactiveRedisCommands).pubsubChannels()
  }

  it should "implement pubsubNumsub" in {
    //given
    val channels: List[String] = genRedisKeys.sample.get
    when(asyncRedisCommands.pubsubNumsub(channels: _*))
      .thenReturn(MockRedisFuture[java.util.Map[String, java.lang.Long]])

    //when
    val t = RedisPubSub.pubsubNumsub(channels: _*)

    //then
    t shouldBe a[Task[Map[String, Long]]]
    verify(asyncRedisCommands).pubsubNumsub(channels: _*)
  }

  it should "implement pubsubNumpat" in {
    //given
    when(asyncRedisCommands.pubsubNumpat()).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisPubSub.pubsubNumpat()

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).pubsubNumpat()
  }

  it should "implement echo" in {
    //given
    val message: V = genRedisValue.sample.get
    when(asyncRedisCommands.echo(message)).thenReturn(MockRedisFuture[Int])

    //when
    val t = RedisPubSub.echo(message)

    //then
    t shouldBe a[Task[Int]]
    verify(asyncRedisCommands).echo(message)
  }

  it should "implement role" in {
    //given
    when(asyncRedisCommands.role()).thenReturn(MockRedisFuture[java.util.List[Any]])

    //when
    val t = RedisPubSub.role()

    //then
    t shouldBe a[Task[List[Any]]]
    verify(asyncRedisCommands).role()
  }

  it should "implement ping" in {
    //given
    when(asyncRedisCommands.ping()).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisPubSub.ping()

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).ping()
  }

  it should "implement readOnly" in {
    //given
    val channel: String = genRedisKey.sample.get
    val message: Int = genRedisValue.sample.get
    when(asyncRedisCommands.readOnly()).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisPubSub.readOnly()

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).readOnly()
  }

  it should "implement readWrite" in {
    //given
    when(asyncRedisCommands.readWrite()).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisPubSub.readWrite()

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).readWrite()
  }

  it should "implement quit" in {
    //given
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(asyncRedisCommands.quit()).thenReturn(MockRedisFuture[String])

    //when
    val t = RedisPubSub.quit()

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).quit()
  }

  it should "implement waitForReplication" in {
    //given
    val replicas: Int = genLong.sample.get.toInt
    val timeout: Long = genLong.sample.get
    when(asyncRedisCommands.waitForReplication(replicas, timeout)).thenReturn(MockRedisFuture[java.lang.Long])

    //when
    val t = RedisPubSub.waitForReplication(replicas, timeout)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).waitForReplication(replicas, timeout)
  }

  //dispatch not supported

}
