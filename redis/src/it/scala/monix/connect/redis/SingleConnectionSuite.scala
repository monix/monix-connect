package monix.connect.redis

import monix.connect.redis.client.{BytesCodec, Codec, RedisConnection, RedisUri}
import monix.connect.redis.test.protobuf.{Person, PersonPk}
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class SingleConnectionSuite extends AsyncFlatSpec with MonixTaskTest with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  override implicit val scheduler: Scheduler = Scheduler.io("single-connection-suite")

  val singleConnection = RedisConnection.standalone(redisUri)

  "RedisStandaloneConnection" should "connect through the uri" in {
    val key: String = Gen.identifier.sample.get
    val value: String = Gen.identifier.sample.get

    RedisConnection.standalone(RedisUri(redisUrl)).connectUtf.use{ cmd =>
      cmd.list.lPush(key, value) >>
       cmd.list.lPop(key)
    }.asserting(_ shouldBe Some(value))
  }

  it should "connect with default utf codecs" in {
    val key: String = Gen.identifier.sample.get
    val value: String = Gen.identifier.sample.get

    singleConnection.connectUtf.use(_.list.lPush(key, value)).runSyncUnsafe()

    val r = singleConnection.connectUtf.use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it can "be used with Custom utf Codecs" in {
    val key: Int = Gen.chooseNum(1, 1000).sample.get
    val value: Int = Gen.chooseNum(1, 1000).sample.get
    implicitly(intUtfCodec) // used implicitly

    singleConnection.connectUtf[Int, Int].use{cmd =>
      cmd.list.lPush(key, value) >>
        cmd.list.lPop(key)
    }.asserting(_ shouldBe Some(value))
  }

  it can "be used with `byteArray` codec by default" in {
    val k: Array[Byte] = Gen.identifier.sample.get.getBytes
    val v: Array[Byte] = Gen.identifier.sample.get.getBytes

    singleConnection.connectByteArray.use { cmd =>
      cmd.list.lPush(k, v) >>
       cmd.list.lPop(k)
    }.asserting(_.get should contain theSameElementsAs v)
  }

  it can "be used with custom byte array Codec for key and value" in {
    implicit val personPkCodec: BytesCodec[PersonPk] = Codec.byteArray[PersonPk](pk => PersonPk.toByteArray(pk), str => PersonPk.parseFrom(str))
    implicit val personCodec: BytesCodec[Person] = Codec.byteArray[Person](person => Person.toByteArray(person), str => Person.parseFrom(str))
    val personPk = genPersonPk.sample.get
    val person = genPerson.sample.get

    singleConnection.connectByteArray[PersonPk, Person].use { cmd =>
      cmd.list.lPush(personPk, person) >> cmd.list.lPop(personPk)
    }.asserting(_ shouldBe Some(person))
  }
}
