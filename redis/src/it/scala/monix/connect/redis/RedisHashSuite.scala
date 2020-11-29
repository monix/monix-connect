package monix.connect.redis

import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually

import scala.util.Failure

class RedisHashSuite extends AnyFlatSpec with Matchers with Eventually {
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

  "hdel" should "return 0 when the hash does not exist at the key" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get

    // when
    RedisKey.del(key).runSyncUnsafe()

    // then
    RedisHash.hdel(key, field).runSyncUnsafe() shouldEqual 0
  }
  it should "return the number of fields deleted" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val value = genRedisValue.sample.get.toString

    // when
    RedisHash.hset(key, field, value).runSyncUnsafe()

    // then
    RedisHash.hdel(key, field).runSyncUnsafe() shouldEqual 1
  }
  it should "not count fields that did not exist" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val missingField = genRedisKey.sample.get
    val value = genRedisValue.sample.get.toString

    // when
    RedisHash.hset(key, field, value).runSyncUnsafe()

    // then
    RedisHash.hdel(key, field, missingField).runSyncUnsafe() shouldEqual 1
  }

  "hexists" should "return true when the field exists" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val value = genRedisValue.sample.get.toString

    // when
    RedisHash.hset(key, field, value).runSyncUnsafe()

    // then
    RedisHash.hexists(key, field).runSyncUnsafe() shouldEqual true
  }
  it should "return false when the field does not exist" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val missingField = genRedisKey.sample.get
    val value = genRedisValue.sample.get.toString

    // when
    RedisHash.hset(key, field, value).runSyncUnsafe()

    // then
    RedisHash.hexists(key, missingField).runSyncUnsafe() shouldEqual false
  }
  it should "return false when the key does not exist" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get

    // when
    RedisKey.del(key).runSyncUnsafe()

    // then
    RedisHash.hexists(key, field).runSyncUnsafe() shouldEqual false
  }

  "hget" should "return the value of the field" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val value = genRedisValue.sample.get.toString

    // when
    RedisHash.hset(key, field, value).runSyncUnsafe()

    // then
    RedisHash.hget(key, field).runSyncUnsafe() shouldEqual value
  }
  it should "return a failed Task when the field does not exist" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val missingField = genRedisKey.sample.get
    val value = genRedisValue.sample.get.toString

    // when
    RedisHash.hset(key, field, value).runSyncUnsafe()

    // then
    val f = RedisHash.hget(key, missingField).runToFuture

    eventually {
      f.value.get.isFailure shouldBe true
      f.value.get shouldBe a[Failure[NoSuchElementException]]
    }
  }
  it should "return a failed Task when the key does not exist" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get

    // when
    RedisKey.del(key).runSyncUnsafe()

    // then
    val f = RedisHash.hget(key, field).runToFuture

    eventually {
      f.value.get.isFailure shouldBe true
      f.value.get shouldBe a[Failure[NoSuchElementException]]
    }
  }



}
