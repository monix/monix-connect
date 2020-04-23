package cloriko.monix.connect.redis

import io.lettuce.core.KeyValue
import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import monix.reactive.Observable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar.mock
import org.mockito.MockitoSugar.verify
import org.mockito.MockitoSugar.when
import org.mockito.IdiomaticMockito
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

class HashRedisSpec
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

  s"${Redis} " should "implement append operation" in {
    //given
    val k: String = genRedisKey()
    val v: Int = genRedisValue()
    when(asyncRedisCommands.append(k, v)).thenReturn(longRedisFuture)

    //when
    val t = Redis.append(k, v)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).append(k, v)
  }

  it should "implement hset" in {
    //given
    val key: String = genRedisKey()
    val field: String = genRedisKey()
    val value: Int = genRedisValue()
    when(connection.async()).thenAnswer(asyncRedisCommands)
    when(asyncRedisCommands.hset(key, field, value)).thenReturn(boolRedisFuture)

    //when
    val t = Redis.hset(key, field, value)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).hset(key, field, value)
  }

  it should "implement hdel operation" in {
    //given
    val k: String = genRedisKey()
    val fields: List[String] = genRedisKeys(3)
    when(asyncRedisCommands.hdel(k, fields: _*)).thenReturn(longRedisFuture)

    //when
    val t = Redis.hdel(k, fields)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).hdel(k, fields: _*)
  }

  it should "implement hexists operation" in {
    //given
    val k: String = genRedisKey()
    val field: String = genRedisKey()
    when(asyncRedisCommands.hexists(k, field)).thenReturn(boolRedisFuture)

    //when
    val t = Redis.hexists(k, field)

    //then
    t shouldBe a[Task[Boolean]]
    verify(asyncRedisCommands).hexists(k, field)
  }

  it should "implement hget operation" in {
    //given
    val k: String = genRedisKey()
    val field: String = genRedisKey()
    when(asyncRedisCommands.hget(k, field)).thenReturn(vRedisFuture)

    //when
    val t = Redis.hget(k, field)

    //then
    t shouldBe a[Task[Int]]
    verify(asyncRedisCommands).hget(k, field)
  }

  it should "implement hgetall operation" in {
    //given
    val k: String = genRedisKey()
    when(asyncRedisCommands.hgetall(k)).thenReturn(mapRedisFuture)

    //when
    val t = Redis.hgetall(k)

    //then
    t shouldBe a[Task[Map[String, String]]]
    verify(asyncRedisCommands).hgetall(k)
  }

  it should "implement hincby operation" in {
    //given
    val k: String = genRedisKey()
    val field: String = genRedisKey()
    val v: Int = genRedisValue()

    when(asyncRedisCommands.hincrby(k, field, v)).thenReturn(longRedisFuture)

    //when
    val t = Redis.hincrby(k, field, v)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).hincrby(k, field, v)
  }

  it should "implement hkeys operation" in {
    //given
    val k: String = genRedisKey()
    when(asyncRedisCommands.hkeys(k)).thenReturn(strListRedisFuture)

    //when
    val t = Redis.hkeys(k)

    //then
    t shouldBe a[Task[List[String]]]
    verify(asyncRedisCommands).hkeys(k)
  }

  it should "implement hlen operation" in {
    //given
    val k: String = genRedisKey()
    when(asyncRedisCommands.hlen(k)).thenReturn(longRedisFuture)

    //when
    val t = Redis.hlen(k)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).hlen(k)
  }

  it should "implement hmget operation" in {
    //given
    val k: String = genRedisKey()
    val fields: List[String] = genRedisKeys(3)
    when(reactiveRedisCommands.hmget(k, fields: _*)).thenReturn(kVFluxRedisFuture)

    //when
    val ob = Redis.hmget(k, fields: _*)

    //then
    ob shouldBe a[Observable[KeyValue[String, Int]]]
    verify(reactiveRedisCommands).hmget(k, fields: _*)
  }

  it should "implement hmset operation" in {

    //given
    val n = 5
    val k: String = genRedisKey()
    val map: Map[String, Int] = genRedisKeys(n).zip(genRedisValues(n)).toMap
    when(asyncRedisCommands.hmset(k, map.asJava)).thenReturn(strRedisFuture)

    //when
    val t = Redis.hmset(k, map)

    //then
    t shouldBe a[Task[String]]
    verify(asyncRedisCommands).hmset(k, map.asJava)
  }

  it should "implement hvals operation" in {
    //given
    val k: String = genRedisKey()
    val field: String = genRedisKey()
    val v: Int = genRedisValue()

    when(asyncRedisCommands.hincrby(k, field, v)).thenReturn(longRedisFuture)

    //when
    val t = Redis.hincrby(k, field, v)

    //then
    t shouldBe a[Task[Long]]
    verify(asyncRedisCommands).hincrby(k, field, v)
  }

}
