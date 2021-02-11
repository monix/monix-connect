package monix.connect.redis

import monix.connect.redis.client.{Codec, Redis, RedisUri}
import monix.connect.redis.test.protobuf.{Person, PersonPk}
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class ClusterIntegrationTest extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  val clusterRedisUri = List("redis://localhost:7001", "redis://localhost:7002", "redis://localhost:7003")
  val clusterRedisUris = List(RedisUri("127.0.0.1", 7000),
    RedisUri("127.0.0.1", 7001),
    RedisUri("127.0.0.1", 7002),
    RedisUri("127.0.0.1", 7003),
    RedisUri("127.0.0.1", 7004),
    RedisUri("127.0.0.1", 7005))

  "ClusterConnection" should "can connect to multiple uri" in {
    //given

    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get
    val clusterCmd = Redis.cluster(clusterRedisUris).utf

    //when
    clusterCmd.use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = clusterCmd.use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "can connect to the cluster with the first uri" in {
    //given
    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get
    val clusterUri = RedisUri("127.0.0.1", 7001)
    val clusterCmd = Redis.cluster(List(clusterUri)).utf

    //when
    clusterCmd.use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = clusterCmd.use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "can connect to the cluster with the last uri" in {
    //given
    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get
    val clusterUri = RedisUri("127.0.0.1", 7005)
    val clusterCmd = Redis.cluster(List(clusterUri)).utf

    //when
    clusterCmd.use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = clusterCmd.use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "can connect to the cluster with (Int, Double) codecs for key and value respectively " in {
    //given
    val key: Int = Gen.choose(1, 1000).sample.get
    val value: Double = Gen.choose(1, 1000).sample.get.toDouble
    val clusterUri = RedisUri("127.0.0.1", 7004)
    val clusterCmd = Redis.cluster[Int, Double](List(clusterUri)).utf(intUtfCodec, doubleUtfCodec)

    //when
    clusterCmd.use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = clusterCmd.use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

  it should "support byte array codecs " in {
    //given
     val personPkCodec: Codec[PersonPk, Array[Byte]] = Codec.byteArray[PersonPk](pk => PersonPk.toByteArray(pk), str => PersonPk.parseFrom(str))
    implicit val personCodec: Codec[Person, Array[Byte]] = Codec.byteArray[Person](person => Person.toByteArray(person), str => Person.parseFrom(str))

    //given
    val personPk = genPersonPk.sample.get
    val person = genPerson.sample.get
    val clusterUri = RedisUri("127.0.0.1", 7003)
    val clusterCmd = Redis.cluster[PersonPk, Person](List(clusterUri)).byteArray(personPkCodec, personCodec)

    //when
    clusterCmd.use(_.list.lPush(personPk, person)).runSyncUnsafe()

    //then
    val r = clusterCmd.use(_.list.lPop(personPk)).runSyncUnsafe()
    Some(person) shouldBe r
  }

}
