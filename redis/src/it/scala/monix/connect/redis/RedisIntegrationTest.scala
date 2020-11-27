package monix.connect.redis

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers
import io.lettuce.core.{RedisClient, ScoredValue}
import io.lettuce.core.api.StatefulRedisConnection
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration._

class RedisIntegrationTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  val redisUrl = "redis://localhost:6379"
  type K = String
  type V = Int
  val genRedisKey: Gen[K] = Gen.identifier
  val genRedisValue: Gen[V] = Gen.choose(0, 10000)
  val genRedisValues: Gen[List[V]] = for {
    n      <- Gen.chooseNum(2, 10)
    values <- Gen.listOfN(n, Gen.choose(0, 10000))
  } yield values

  implicit val connection: StatefulRedisConnection[String, String] = RedisClient.create(redisUrl).connect()

  s"${RedisHash}" should "access non existing key in redis and get None" in {
    //given
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get

    //when
    val t: Task[Option[String]] = RedisHash.hget(key, field)

    //then
    val r: Option[String] = t.runSyncUnsafe()
    r shouldBe None
  }

  s"${RedisString} " should "insert a string into the given key and get its size from redis" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get.toString

    //when
    RedisString.set(key, value).runSyncUnsafe()

    //and
    val t: Task[Long] = RedisString.strlen(key)

    //then
    val lenght: Long = t.runSyncUnsafe()
    lenght shouldBe value.length
  }

  s"${RedisServer} " should "insert a string into key and get its size from redis" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get.toString

    //when
    RedisString.set(key, value).runSyncUnsafe()
    val beforeFlush: Long = Redis.exists(key).runSyncUnsafe()

    //and
    RedisServer.flushallAsync().runSyncUnsafe()
    val afterFlush: Task[Long] = Redis.exists(key)

    //then
    beforeFlush shouldEqual 1L
    afterFlush.runSyncUnsafe() shouldEqual 0L
  }

  s"${RedisHash}" should "insert a single element into a hash and read it back" in {
    //given
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get.toString

    //when
    RedisHash.hset(key, field, value).runSyncUnsafe()

    //and
    val t: Task[Option[String]] = RedisHash.hget(key, field)

    //then
    val r: Option[String] = t.runSyncUnsafe()
    r shouldBe Some(value)
  }

  s"${RedisKey}" should "handles ttl correctly" in {
    //given
    val key1: K = genRedisKey.sample.get
    val key2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get.toString
    RedisString.set(key1, value).runSyncUnsafe()

    //when
    val (initialTtl, expire, finalTtl, existsWithinTtl, existsRenamed, existsAfterFiveSeconds) = {
      for {
        initialTtl             <- RedisKey.ttl(key1)
        expire                 <- RedisKey.expire(key1, 2)
        finalTtl               <- RedisKey.ttl(key1)
        existsWithinTtl        <- RedisKey.exists(key1)
        _                      <- RedisKey.rename(key1, key2)
        existsRenamed          <- RedisKey.exists(key2)
        _                      <- Task.sleep(3.seconds)
        existsAfterFiveSeconds <- RedisKey.exists(key2)
      } yield (initialTtl, expire, finalTtl, existsWithinTtl, existsRenamed, existsAfterFiveSeconds)
    }.runSyncUnsafe()

    //then
    initialTtl should be < 0L
    finalTtl should be > 0L
    expire shouldBe true
    existsWithinTtl shouldBe 1L
    existsRenamed shouldBe 1L
    existsAfterFiveSeconds shouldBe 0L
  }

  s"${RedisList}" should "insert elements into a list and reading back the same elements" in {
    //given
    val key: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get.map(_.toString)

    //when
    RedisList.lpush(key, values: _*).runSyncUnsafe()

    //and
    val ob: Observable[String] = RedisList.lrange(key, 0, values.size)

    //then
    val result: List[String] = ob.toListL.runSyncUnsafe()
    result should contain theSameElementsAs values
  }

  s"${RedisSet}" should "allow to compose nice for comprehensions" in {
    //given
    val k1: K = genRedisKey.sample.get
    val m1: List[String] = genRedisValues.sample.get.map(_.toString)
    val k2: K = genRedisKey.sample.get
    val m2: List[String] = genRedisValues.sample.get.map(_.toString)
    val k3: K = genRedisKey.sample.get

    //when
    val (size1, size2, moved) = {
      for {
        size1 <- RedisSet.sadd(k1, m1: _*)
        size2 <- RedisSet.sadd(k2, m2: _*)
        _     <- RedisSet.sadd(k3, m1: _*)
        moved <- RedisSet.smove(k1, k2, m1.head)
      } yield {
        (size1, size2, moved)
      }
    }.runSyncUnsafe()

    //and
    val (s1, s2, union, diff) = {
      for {
        s1    <- RedisSet.smembers(k1).toListL
        s2    <- RedisSet.smembers(k2).toListL
        union <- RedisSet.sunion(k1, k2).toListL
        diff  <- RedisSet.sdiff(k3, k1).toListL
      } yield (s1, s2, union, diff)
    }.runSyncUnsafe()

    //then
    size1 shouldBe m1.size
    s1.size shouldEqual (m1.size - 1)
    size2 shouldBe m2.size
    s2.size shouldEqual (m2.size + 1)
    moved shouldBe true
    s1 shouldNot contain theSameElementsAs m1
    s2 shouldNot contain theSameElementsAs m2
    union should contain theSameElementsAs m1 ++ m2
    //although the list are not equal as at the beginning because of the move operation, its union still is the same
    diff should contain theSameElementsAs List(m1.head)
    //the difference between the k3 and k1 is equal to the element that was moved
  }

  s"${RedisSortedSet}" should "insert elements into a with no order and reading back sorted" in {
    //given
    val k: K = genRedisKey.sample.get
    val v0: String = Gen.alphaLowerStr.sample.get
    val v1: String = Gen.alphaLowerStr.sample.get
    val v2: String = Gen.alphaLowerStr.sample.get
    val minScore: Double = 1
    val middleScore: Double = 3
    val maxScore: Double = 4
    val incrby: Double = 2

    //when
    RedisSortedSet.zadd(k, minScore, v0)
    val t: Task[(ScoredValue[String], ScoredValue[String])] = for {
      _   <- RedisSortedSet.zadd(k, minScore, v0)
      _   <- RedisSortedSet.zadd(k, middleScore, v1)
      _   <- RedisSortedSet.zadd(k, maxScore, v2)
      _   <- RedisSortedSet.zincrby(k, incrby, v1)
      min <- RedisSortedSet.zpopmin(k)
      max <- RedisSortedSet.zpopmax(k)
    } yield (min, max)

    //then
    val (min, max) = t.runSyncUnsafe()
    min.getScore shouldBe minScore
    min.getValue shouldBe v0
    max.getScore shouldBe middleScore + incrby
    max.getValue shouldBe v1
  }

  s"${Redis}" should "allow to composition of different Redis submodules" in {
    //given
    val k1: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get.toString
    val k2: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get.map(_.toString)
    val k3: K = genRedisKey.sample.get

    val (v: String, len: Long, l: List[String], keys: List[String]) = {
      for {
        _    <- Redis.flushallAsync()
        _    <- RedisKey.touch(k1)
        _    <- RedisString.set(k1, value)
        _    <- RedisKey.rename(k1, k2)
        _    <- RedisList.lpush(k3, values: _*)
        v    <- RedisString.get(k2): Task[String]
        _    <- RedisList.lpushx(k3, v)
        _    <- RedisKey.del(k2)
        len  <- RedisList.llen(k3): Task[Long]
        l    <- RedisList.lrange(k3, 0, len).toListL
        keys <- Redis.keys("*").toListL
      } yield (v, len, l, keys)
    }.runSyncUnsafe()

    v shouldBe value
    len shouldBe values.size + 1
    l should contain theSameElementsAs value :: values
    keys.size shouldBe 1
    keys.head shouldBe k3
  }

}
