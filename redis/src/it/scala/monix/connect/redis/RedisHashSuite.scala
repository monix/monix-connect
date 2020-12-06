package monix.connect.redis

import io.lettuce.core.{RedisClient, RedisCommandExecutionException}
import io.lettuce.core.api.StatefulRedisConnection
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import scala.jdk.CollectionConverters._
import scala.util.Failure

class RedisHashSuite extends AnyFlatSpec
  with RedisIntegrationFixture with Matchers with Eventually {

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
      f.value.get.failed.get shouldBe a[NoSuchElementException]
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
      f.value.get.failed.get shouldBe a[NoSuchElementException]
    }
  }

  "hincrby" should "start from 0, and increment the value" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val increment = genLong.sample.get

    // then
    RedisHash.hincrby(key, field, increment).runSyncUnsafe() shouldEqual increment
  }

  "hincrbyfloat" should "start from 0, and increment the value" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val increment = genDouble.sample.get

    // then
    RedisHash.hincrbyfloat(key, field, increment).runSyncUnsafe() shouldEqual increment
  }
  it should "increment an integer" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisKey.sample.get
    val baseValue: Long = genLong.sample.get
    val increment: Double = genDouble.sample.get

    // when
    RedisHash.hincrby(key, field, baseValue).runSyncUnsafe()

    // then
    // NOTE: (de)serialization result in small precision errors with double precision
    val expectedValue: Double = baseValue.doubleValue() + increment
    RedisHash.hincrbyfloat(key, field, increment).runSyncUnsafe().floatValue() shouldEqual expectedValue.floatValue()
  }

  "hgetall" should "return all field-value pairs" in {
    // given
    val key = genRedisKey.sample.get
    val fields = Gen.nonEmptyMap(genRedisPair).sample.get

    // when
    RedisHash.hmset(key, fields).runSyncUnsafe()

    // then
    RedisHash.hgetall(key).runSyncUnsafe() shouldEqual fields
  }
  it should "return an empty Map when the key does not exist" in {
    // given
    val key = genRedisKey.sample.get

    // then
    RedisHash.hgetall(key).runSyncUnsafe() shouldEqual Map.empty
  }

  "hkeys" should "return all the fields at a key" in {
    // given
    val key = genRedisKey.sample.get
    val fields = Gen.nonEmptyMap(genRedisPair).sample.get

    // when
    RedisHash.hmset(key, fields).runSyncUnsafe()

    // then
    RedisHash.hkeys(key).toListL.runSyncUnsafe().sorted shouldEqual fields.keys.toList.sorted
  }
  it should "return an empty list when the key does not exist" in {
    // given
    val key = genRedisKey.sample.get

    // then
    RedisHash.hkeys(key).toListL.runSyncUnsafe() shouldEqual List.empty
  }

  "hlen" should "return the number of fields at the key" in {
    // given
    val key = genRedisKey.sample.get
    val fields = Gen.nonEmptyMap(genRedisPair).sample.get

    // when
    RedisHash.hmset(key, fields).runSyncUnsafe()

    // then
    RedisHash.hlen(key).runSyncUnsafe() shouldEqual fields.size
  }
  it should "return a failed Task the value is not a hash" in {
    // given
    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get

    // when
    RedisString.set(key, value).runSyncUnsafe()

    // then
    val f = RedisHash.hlen(key).runToFuture

    eventually {
      f.value.get.failed.get shouldBe a[RedisCommandExecutionException]
    }
  }
  it should "return 0 when the key does not exist" in {
    // given
    val key = genRedisKey.sample.get

    // then
    RedisHash.hlen(key).runSyncUnsafe() shouldEqual 0
  }

  "hmget" should "return all the specified fields" in {
    // given
    val key = genRedisKey.sample.get
    val fields = Gen.nonEmptyMap(genRedisPair).sample.get
    val fieldsToGet = fields.take(2)

    // when
    RedisHash.hmset(key, fields).runSyncUnsafe()

    // then
    val result = RedisHash.hmget(key, fieldsToGet.keys.toList:_*).toListL.runSyncUnsafe()
    result.map(_.tuple).toMap shouldEqual fieldsToGet
  }
  it should "return an empty map when the requested fields don't exist" in {
    // given
    val key = genRedisKey.sample.get
    val fields = Gen.nonEmptyMap(genRedisPair).sample.get
    val fieldsToGet = Gen.listOfN(2, genRedisKey).sample.get

    // when
    RedisHash.hmset(key, fields).runSyncUnsafe()

    // then
    val result = RedisHash.hmget(key, fieldsToGet:_*).toListL.runSyncUnsafe()
    result.map(_.tuple).toMap shouldEqual Map.empty
  }
  it should "return a failed Task when the key doesn't exist" in {
    // given
    val key = genRedisKey.sample.get
    val fieldsToGet = Gen.listOfN(2, genRedisKey).sample.get

    // then
    val f = RedisHash.hmget(key, fieldsToGet:_*).toListL.runToFuture
    eventually {
      f.value.get.failed.get shouldBe a[NoSuchElementException]
    }
  }

  "hscan" should "return a key value cursor containing the fields" in {
    // given
    val key = genRedisKey.sample.get
    val fields = Gen.nonEmptyMap(genRedisPair).sample.get

    // when
    RedisHash.hmset(key, fields).runSyncUnsafe()

    // then
    val result = RedisHash.hscan(key).runSyncUnsafe().getMap
    result.asScala.toList.sorted shouldEqual fields.toList.sorted
  }
  it should "return an empty map when the key does not exist" in {
    // given
    val key = genRedisKey.sample.get

    // then
    val result = RedisHash.hscan(key).runSyncUnsafe()
    result.getMap.asScala shouldEqual Map.empty
  }

  "hset" should "set the string value" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisPair.sample.get

    // then
    val result = RedisHash.hset(key, field._1, field._2).runSyncUnsafe()
    result shouldEqual true
  }
  it should "update an existing field" in {
    // given
    val key = genRedisKey.sample.get
    val field = genRedisPair.sample.get
    val newValue = genRedisValue.sample.get

    // when
    RedisHash.hset(key, field._1, field._2).runSyncUnsafe()

    // then
    val result = RedisHash.hset(key, field._1, newValue).runSyncUnsafe()
    result shouldEqual false
  }
  it should "fail when trying to set a field on a string value" in {
    // given
    val key = genRedisKey.sample.get
    val stringValue = genRedisValue.sample.get
    val fieldPair = genRedisPair.sample.get

    // when
    RedisString.set(key, stringValue).runSyncUnsafe()

    // then
    val f = RedisHash.hset(key, fieldPair._1, fieldPair._2).runToFuture

    eventually {
      f.value.get.failed.get shouldBe a[RedisCommandExecutionException]
    }
  }



}
