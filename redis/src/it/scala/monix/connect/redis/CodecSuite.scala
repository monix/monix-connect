package monix.connect.redis

import monix.connect.redis.client.{Codec, Redis}
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

  val connection = Redis.single(redisUri)

  "A byte array codec" should "encode and decode protobuf keys and values" in {

    implicit val personPkCodec: Codec[PersonPk, Array[Byte]] = Codec.byteArray[PersonPk](pk => PersonPk.toByteArray(pk), str => PersonPk.parseFrom(str))
    implicit val personCodec: Codec[Person, Array[Byte]] = Codec.byteArray[Person](person => Person.toByteArray(person), str => Person.parseFrom(str))

    //given
    val personPk = genPersonPk.sample.get
    val person = genPerson.sample.get

    //when
    connection.byteArray[PersonPk, Person].use(_.list.lPush(personPk, person)).runSyncUnsafe()

    //then
    val r = connection.byteArray[PersonPk, Person].use(_.list.lPop(personPk)).runSyncUnsafe()
    Some(person) shouldBe r
  }

  s"An utf codec" should "encode and decode int values" in {
    //given
    val key: Int = Gen.chooseNum(1, 1000).sample.get
    val value: Int = Gen.chooseNum(1, 1000).sample.get
    implicitly(intUtfCodec) // used implicitly

    //when
    connection.utf[Int, Int].use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = connection.utf[Int, Int].use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "encode and decode int keys with strings api" in {
    //given
    val key: Int = Gen.chooseNum(1, 10000).sample.get
    val n: Int = Gen.chooseNum(1, 99).sample.get
    implicitly(intUtfCodec) // used implicitly

    //when
    val r = connection.utf[Int, Int].use(cmd =>
      for {
        _ <- Observable(n, n, n).mapEval(cmd.string.append(key, _)).completedL
        r <- cmd.string.get(key)
      } yield r
    ).runSyncUnsafe()

    //then
    r shouldBe Some(s"$n$n$n".toInt)
  }



}
