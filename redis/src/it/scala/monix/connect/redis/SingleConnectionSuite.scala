package monix.connect.redis

import monix.connect.redis.client.{Codec, RedisConnection, RedisUri}
import monix.connect.redis.test.protobuf.{Person, PersonPk}
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class SingleConnectionSuite extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  val singleConnection = RedisConnection.single(redisUri)

  override def beforeEach(): Unit = {
    super.beforeEach()
    singleConnection.connectUtf.use(_.server.flushAll()).runSyncUnsafe()
  }

  it should "connect through the uri" in {
    //given
    val key: String = Gen.identifier.sample.get
    val value: String = Gen.identifier.sample.get

    //when
    RedisConnection.single(RedisUri(redisUrl)).connectUtf.use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = RedisConnection.single(RedisUri(redisUrl)).connectUtf.use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "connect with default utf codecs" in {
    //given
    val key: String = Gen.identifier.sample.get
    val value: String = Gen.identifier.sample.get

    //when
    singleConnection.connectUtf.use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = singleConnection.connectUtf.use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it can " be used with Custom utf Codecs" in {
    //given
    val key: Int = Gen.chooseNum(1, 1000).sample.get
    val value: Int = Gen.chooseNum(1, 1000).sample.get
    implicitly(intUtfCodec) // used implicitly

    //when
    singleConnection.connectUtf[Int, Int].use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = singleConnection.connectUtf[Int, Int].use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it can "be used with `byteArray` codec by default" in {
    //given
    val k: Array[Byte] = Gen.identifier.sample.get.getBytes
    val v: Array[Byte] = Gen.identifier.sample.get.getBytes

    //when
    singleConnection.connectByteArray.use(_.list.lPush(k, v)).runSyncUnsafe()

    //then
    val r = singleConnection.connectByteArray.use(_.list.lPop(k)).runSyncUnsafe()
    v should contain theSameElementsAs r.get
  }

  it can "be used with custom byte array Codec for key and value" in {
    //given
    implicit val personPkCodec: Codec[PersonPk, Array[Byte]] = Codec.byteArray[PersonPk](pk => PersonPk.toByteArray(pk), str => PersonPk.parseFrom(str))
    implicit val personCodec: Codec[Person, Array[Byte]] = Codec.byteArray[Person](person => Person.toByteArray(person), str => Person.parseFrom(str))
    val personPk = genPersonPk.sample.get
    val person = genPerson.sample.get

    //when
    singleConnection.connectByteArray[PersonPk, Person].use(_.list.lPush(personPk, person)).runSyncUnsafe()

    //then
    val r = singleConnection.connectByteArray[PersonPk, Person].use(_.list.lPop(personPk)).runSyncUnsafe()
    Some(person) shouldBe r
  }

}
