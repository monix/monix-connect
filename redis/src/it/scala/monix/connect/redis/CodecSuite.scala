package monix.connect.redis

import monix.connect.redis.client.{BytesCodec, Codec, RedisConnection}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import monix.reactive.Observable
import org.scalacheck.Gen

import scala.concurrent.duration._
import monix.connect.redis.test.protobuf.{Person, PersonPk}
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskTest

class CodecSuite extends AsyncFlatSpec with MonixTaskTest with  RedisIntegrationFixture with Matchers with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  override implicit val scheduler: Scheduler = Scheduler.io("codec-suite")

  "A byte array codec" should "encode and decode protobuf keys and values" in {
    implicit val personPkCodec: BytesCodec[PersonPk] = Codec.byteArray(pk => PersonPk.toByteArray(pk), bytes => PersonPk.parseFrom(bytes))
    implicit val personCodec: BytesCodec[Person] = Codec.byteArray(person => Person.toByteArray(person), bytes => Person.parseFrom(bytes))
    val personPk = genPersonPk.sample.get
    val person = genPerson.sample.get

    redis.connectByteArray[PersonPk, Person].use{ cmd =>
      cmd.list.lPush(personPk, person) >>
        cmd.list.lPop(personPk)
    }.asserting(_ shouldBe Some(person))
  }

  it should "connect to the cluster en/decoding keys and values as byte array" in {
    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get

    redis.connectByteArray.use { cmd =>
      cmd.list.lPush(key.getBytes, value.getBytes) >>
        cmd.list.lPop(key.getBytes)
    }.asserting(_.get shouldBe value.getBytes)
  }

  "An utf codec" should "encode and decode int numbers" in {
    val key: Int = Gen.chooseNum(1, 1000000).sample.get
    val value: Int = Gen.chooseNum(1, 1000).sample.get
    implicitly(intUtfCodec)

    redis.connectUtf[Int, Int](intUtfCodec, intUtfCodec).use { cmd =>
      cmd.list.lPush(key, value) >>
        cmd.list.lPop(key)
    }.asserting(_ shouldBe Some(value))
  }

  it should "encode and decode int keys with strings api" in {
    val key: Int = Gen.chooseNum(1, 10000000).sample.get
    val n: Int = Gen.chooseNum(1, 99).sample.get
    implicitly(intUtfCodec) // used implicitly

    redis.connectUtf[Int, Int].use(cmd =>
      Observable(n, n, n).mapEval(cmd.string.append(key, _)).completedL >>
        cmd.string.get(key)
    ).asserting(_ shouldBe Some(s"$n$n$n".toInt))
  }

  it should "encode and decode float numbers" in {
    val key: Float = Gen.chooseNum[Float](1, 10000000).sample.get
    val value: Float = Gen.chooseNum[Float](1, 100000).sample.get
    implicitly(floatUtfCodec) // used implicitly

    redis.connectUtf[Float, Float].use(cmd =>
      cmd.list.lPush(key, value) >>
        cmd.list.lPop(key)
    ).asserting(_ shouldBe Some(value))
  }

  it should "encode and decode double numbers" in {
    val key: Double = Gen.chooseNum[Double](1, 100000000).sample.get
    val value: Double = Gen.chooseNum[Double](1, 10000).sample.get
    implicitly(doubleUtfCodec) // used implicitly

    redis.connectUtf[Double, Double].use(cmd =>
      cmd.list.lPush(key, value) >> cmd.list.lPop(key)
    ).asserting(_ shouldBe Some(value))
  }

  it should "encode and decode big ints" in {
    val key: BigInt = BigInt(Gen.chooseNum(1, 100000000).sample.get)
    val value: BigInt = BigInt.apply(Gen.chooseNum(1, 10000).sample.get)
    implicitly(bigIntUtfCodec) // used implicitly

    redis.connectUtf[BigInt, BigInt].use(cmd =>
      cmd.list.lPush(key, value) >> cmd.list.lPop(key)).asserting(_ shouldBe Some(value))
  }

  it should "encode and decode big decimals" in {
    val key: BigDecimal = BigDecimal(Gen.chooseNum[Double](1, 10000000).sample.get)
    val value: BigDecimal = BigDecimal(Gen.chooseNum[Double](1, 1000).sample.get)
    implicitly(bigDecimalUtfCodec) // used implicitly

    redis.connectUtf[BigDecimal, BigDecimal].use(cmd =>
      cmd.list.lPush(key, value) >> cmd.list.lPop(key)
    ).asserting(_  shouldBe Some(value))
  }

}
