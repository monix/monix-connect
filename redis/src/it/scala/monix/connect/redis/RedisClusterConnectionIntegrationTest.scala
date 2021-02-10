package monix.connect.redis

import monix.connect.redis.client.{Codec, RedisClusterConnection, RedisUri}
import monix.connect.redis.test.protobuf.{Person, PersonPk}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class RedisClusterConnectionIntegrationTest extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

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

    //when
    RedisClusterConnection.create(clusterRedisUris).use(_.list.lPush(key, value)).runSyncUnsafe()

    //then
    val r = RedisClusterConnection.create(clusterRedisUris).use(_.list.lPop(key)).runSyncUnsafe()
    Some(value) shouldBe r
  }

}
