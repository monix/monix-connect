package monix.connect.redis

import monix.connect.redis.client.{BytesCodec, Codec, RedisConnection, RedisUri}
import monix.connect.redis.test.protobuf.{Person, PersonPk}
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ClusterConnectionSuite extends AsyncFlatSpec with MonixTaskSpec
  with RedisIntegrationFixture
  with Matchers
  with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  override implicit val scheduler: Scheduler = Scheduler.io("cluster-connection-suite")

  "ClusterConnection" should "can connect to multiple uri" in {
    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get
    RedisConnection.cluster(clusterRedisUris).connectUtf.use { cmd =>
      cmd.list.lPush(key, value) >>
        cmd.list.lPop(key)
    }.asserting(_ shouldBe Some(value))
  }

  it can "connect to the cluster with the first uri" in {
    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get
    val clusterUri = RedisUri("127.0.0.1", 7001)
    RedisConnection.cluster(List(clusterUri)).connectUtf.use { cmd =>
      cmd.list.lPush(key, value) >>
        cmd.list.lPop(key)
    }.asserting(_ shouldBe Some(value))
  }

  it can "connect to the cluster with the last uri" in {
    val key = genRedisKey.sample.get
    val value = genRedisValue.sample.get
    val clusterUri = RedisUri("127.0.0.1", 7005)
    RedisConnection.cluster(List(clusterUri)).connectUtf.use { cmd =>
      cmd.list.lPush(key, value) >>
        cmd.list.lPop(key)
    }.asserting(_ shouldBe Some(value))
  }

  it can "connect to the cluster with (Int, Double) codecs for key and value respectively " in {
    val key: Int = Gen.choose(1, 1000).sample.get
    val value: Double = Gen.choose(1, 1000).sample.get.toDouble
    val clusterUri = RedisUri("127.0.0.1", 7004)
    RedisConnection.cluster[Int, Double](List(clusterUri)).connectUtf(intUtfCodec, doubleUtfCodec).use{ cmd =>
      cmd.list.lPush(key, value) >>
        cmd.list.lPop(key)
    }.asserting(_ shouldBe Some(value))
  }

  it should "support byte array codecs " in {
    implicit val personPkCodec: BytesCodec[PersonPk] = Codec.byteArray[PersonPk](pk => PersonPk.toByteArray(pk), str => PersonPk.parseFrom(str))
    implicit val personCodec: BytesCodec[Person] = Codec.byteArray[Person](person => Person.toByteArray(person), str => Person.parseFrom(str))

    val personPk = genPersonPk.sample.get
    val person = genPerson.sample.get
    val clusterUri = RedisUri("127.0.0.1", 7003)
    RedisConnection.cluster[PersonPk, Person](List(clusterUri))
      .connectByteArray(personPkCodec, personCodec).use { cmd =>
        cmd.list.lPush(personPk, person) >>
          cmd.list.lPop(personPk)
    }.asserting(_ shouldBe Some(person))
  }

}
