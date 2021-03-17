package monix.connect.redis

import monix.connect.redis.client.{Codec, RedisConnection}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen

import scala.concurrent.duration._
import monix.connect.redis.test.protobuf.{Person, PersonPk}

class CodecSuite extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  val connection = RedisConnection.single(redisUri)
  override def beforeEach(): Unit = {
    super.beforeEach()
    connection.connectUtf.use(_.server.flushAll()).runSyncUnsafe()
  }

  "A byte array codec" should "encode and decode protobuf keys and values" in {
    implicit val personPkCodec: Codec[PersonPk, Array[Byte]] = Codec.byteArray[PersonPk](pk => PersonPk.toByteArray(pk), str => PersonPk.parseFrom(str))
    implicit val personCodec: Codec[Person, Array[Byte]] = Codec.byteArray[Person](person => Person.toByteArray(person), str => Person.parseFrom(str))

    //given
    val personPk = genPersonPk.sample.get
    val person = genPerson.sample.get

    //when
    connection.connectByteArray[PersonPk, Person].use(_.list.lPush(personPk, person)).runSyncUnsafe()

    //then
    val r = connection.connectByteArray[PersonPk, Person].use(_.list.lPop(personPk)).runSyncUnsafe()
    Some(person) shouldBe r
  }

  it should "connect to the cluster en/decoding keys and values as byte array" in {
    //given
    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get

    //when
    connection.connectByteArray.use(_.list.lPush(key.getBytes, value.getBytes)).runSyncUnsafe()

    //then
    val r = connection.connectByteArray.use(_.list.lPop(key.getBytes)).runSyncUnsafe()
    r.get shouldBe value.getBytes
  }

  "An utf codec" should "encode and decode int numbers" in {
    //given
    val key: Int = Gen.chooseNum(1, 1000).sample.get
    val value: Int = Gen.chooseNum(1, 1000).sample.get
    implicitly(intUtfCodec) // used implicitly

    //when
    connection.connectUtf[Int, Int](intUtfCodec, intUtfCodec).use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = connection.connectUtf[Int, Int].use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "encode and decode int keys with strings api" in {
    //given
    val key: Int = Gen.chooseNum(1, 10000).sample.get
    val n: Int = Gen.chooseNum(1, 99).sample.get
    implicitly(intUtfCodec) // used implicitly

    //when
    val r = connection.connectUtf[Int, Int].use(cmd =>
      for {
        _ <- Observable(n, n, n).mapEval(cmd.string.append(key, _)).completedL
        r <- cmd.string.get(key)
      } yield r
    ).runSyncUnsafe()

    //then
    r shouldBe Some(s"$n$n$n".toInt)
  }

  it should "encode and decode float numbers" in {
    //given
    val key: Float = Gen.chooseNum[Float](1, 1000).sample.get
    val value: Float = Gen.chooseNum[Float](1, 1000).sample.get
    implicitly(floatUtfCodec) // used implicitly

    //when
    connection.connectUtf[Float, Float].use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = connection.connectUtf[Float, Float].use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "encode and decode double numbers" in {
    //given
    val key: Double = Gen.chooseNum[Double](1, 1000).sample.get
    val value: Double = Gen.chooseNum[Double](1, 1000).sample.get
    implicitly(doubleUtfCodec) // used implicitly

    //when
    connection.connectUtf[Double, Double].use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = connection.connectUtf[Double, Double].use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "encode and decode big ints" in {
    //given
    val key: BigInt = BigInt(Gen.chooseNum(1, 1000).sample.get)
    val value: BigInt = BigInt.apply(Gen.chooseNum(1, 1000).sample.get)
    implicitly(bigIntUtfCodec) // used implicitly

    //when
    connection.connectUtf[BigInt, BigInt].use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = connection.connectUtf[BigInt, BigInt].use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "encode and decode big decimals" in {
    //given
    val key: BigDecimal = BigDecimal(Gen.chooseNum[Double](1, 1000).sample.get)
    val value: BigDecimal = BigDecimal(Gen.chooseNum[Double](1, 1000).sample.get)
    implicitly(bigDecimalUtfCodec) // used implicitly

    //when
    connection.connectUtf[BigDecimal, BigDecimal].use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = connection.connectUtf[BigDecimal, BigDecimal].use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

}
