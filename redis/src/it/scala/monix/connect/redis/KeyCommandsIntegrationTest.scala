package monix.connect.redis

import cats.data.NonEmptyList
import io.lettuce.core.ScoredValue
import monix.connect.redis.client.{Redis, RedisCmd}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.time.Instant
import java.util.Date
import scala.concurrent.duration._

class KeyCommandsIntegrationTest extends AnyFlatSpec
  with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    redisClient.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  s"${KeyCommands}" should "deletes key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use(cmd => cmd.string.set(k1, value) *> cmd.string.set(k2, value)).runSyncUnsafe()

    //when
    val (d1: Long, d2: Long) = Redis.connect(redisUrl).use { cmd =>
      for {
        d1 <- cmd.key.del(k1, k2)
        d2 <- cmd.key.del(k3)
      } yield (d1, d2)
    }.runSyncUnsafe()

    //then
    d1 shouldBe 2L
    d2 shouldBe 0L
  }

  it  should "unlinks key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use(cmd => cmd.string.set(k1, value) *> cmd.string.set(k2, value)).runSyncUnsafe()

    //when
    val (r1: Long, r2: Long) = Redis.connect(redisUrl).use { cmd =>
      for {
        r1 <- cmd.key.unLink(k1, k2)
        r2 <- cmd.key.unLink(k3)
      } yield (r1, r2)
    }.runSyncUnsafe()

    //then
    r1 shouldBe 2L
    r2 shouldBe 0L
  }

  it  should "dumps the serialized value stored at the key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use(cmd => cmd.string.set(k1, value)).runSyncUnsafe()

    //when
    val (d1, d2) = Redis.connect(redisUrl).use { cmd =>
      for {
        d1 <- cmd.key.dump(k1)
        d2 <- cmd.key.dump(k2)
      } yield (d1, d2)
    }.runSyncUnsafe()

    //then
    d1 should not be empty
    d2 shouldBe empty
  }

  "exists" should "count the number of existing keys" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use(cmd => cmd.string.set(k1, value) *> cmd.string.set(k2, value)).runSyncUnsafe()

    //when
    val (r1, r2) = Redis.connect(redisUrl).use { cmd =>
      for {
        r1 <- cmd.key.exists(k1, k2)
        r2 <- cmd.key.exists(k3)
      } yield (r1, r2)
    }.runSyncUnsafe()

    //then
    r1 shouldBe 2L
    r2 shouldBe 0L
  }

  "expire" should "set the expiration of a key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use(cmd => cmd.string.set(k1, value)).runSyncUnsafe()

    //when
    val (r1, r2, r3, r4) = Redis.connect(redisUrl).use { cmd =>
      for {
        r1 <- cmd.key.expire(k1, 100)
        r2 <- cmd.key.expire(k2, 100)
        r3 <- cmd.key.expire(k1, 100.seconds)
        r4 <- cmd.key.expire(k2, 100.seconds)
      } yield (r1, r2, r3, r4)
    }.runSyncUnsafe()

    //then
    r1 shouldBe true
    r2 shouldBe false
    r3 shouldBe true
    r4 shouldBe false
  }

  "expireAt" should "count the number of existing keys" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get

    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use(cmd => cmd.string.set(k1, value) >> cmd.string.set(k2, value)).runSyncUnsafe()

    //when
    val (r1, r1Updated, r2, r3, r3Updated, r4, ttl1Before, ttl1After, ttl3Before, ttl3After) = Redis.connect(redisUrl).use { cmd =>
      for {
        r1 <- cmd.key.expireAt(k1, new Date())
        ttl1Before <- cmd.key.ttl(k1)
        r1Updated <- cmd.key.expireAt(k1, Instant.now().toEpochMilli)
        ttl1After <- cmd.key.ttl(k1)
        r2 <- cmd.key.expireAt(k3, new Date())
        r3 <- cmd.key.expireAt(k2, Instant.now().toEpochMilli)
        ttl3Before <- cmd.key.ttl(k1)
        r3Updated <- cmd.key.expireAt(k2, Instant.now().toEpochMilli + 10.seconds.toMillis)
        ttl3After <- cmd.key.ttl(k1)
        r4 <- cmd.key.expireAt(k3, Instant.now().toEpochMilli)
      } yield (r1, r1Updated, r2, r3, r3Updated, r4, ttl1Before, ttl1After, ttl3Before, ttl3After)
    }.runSyncUnsafe()

    //then
    r1 shouldBe true
    r1Updated shouldBe false //the expiration was already set with expireAt(Date) and fails if we try on the same key with `expireAt(epochMillis)`
    ttl1Before shouldBe ttl1After
    r2 shouldBe false

    //and
    r3 shouldBe true
    r3Updated shouldBe true //updates expiration
    ttl3Before shouldBe ttl3After
    r4 shouldBe false //did not exited
  }

  "keys" should "get only matched keys" in {
    //given
    val prefix = "prefix_"
    val k1: K = prefix + genRedisKey.sample.get
    val k2: K = prefix + genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use{ cmd =>
      cmd.string.set(k1, value) *>
        cmd.string.set(k2, value) *>
          cmd.string.set(k3, value)
    }.runSyncUnsafe()

    //when
    val l = Redis.connect(redisUrl).use(_.key.keys(s"$prefix*" ).toListL).runSyncUnsafe()

    //then
    List(k1, k2) should contain theSameElementsAs l
  }

  //todo add tests
  /*
  "migrate" should "transfer a key from a redis instance to another one" in {
    //given
    val prefix = "prefix_"
    val k1: K = prefix + genRedisKey.sample.get
    val k2: K = prefix + genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use{ cmd =>
      cmd.string.set(k1, value) *>
        cmd.string.set(k2, value) *>
        cmd.string.set(k3, value)
    }.runSyncUnsafe()

    //when
    val l = Redis.connect(redisUrl).use(_.key.keys(s"$prefix*" ).toListL).runSyncUnsafe()

    //then
    List(k1, k2) should contain theSameElementsAs l
  }*/

  it should "move a key to another database" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    Redis.connect(redisUrl).use{ cmd =>
      cmd.string.set(k1, value)
    }.runSyncUnsafe()

    //when
    val (moved1, moved2) = Redis.connect(redisUrl).use(cmd =>
      Task.parZip2(cmd.key.move(k1, 2), cmd.key.move(k2, 2))
    ).runSyncUnsafe()

    //then
    moved1 shouldBe true
    moved2 shouldBe false // the key did not existed
  }

  it should "if exists, show the internal encoding representation " in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val v1: V = genRedisValue.sample.get
    val v2: String = "Sample String"

    Redis.connect(redisUrl).use(cmd =>
      cmd.string.set(k1, v1) >> cmd.string.set(k2, v2)
    ).runSyncUnsafe()

    //when
    val (r1, r2, r3) = Redis.connect(redisUrl).use(cmd =>
      Task.parZip3(cmd.key.objectEncoding(k1),
        cmd.key.objectEncoding(k2),
        cmd.key.objectEncoding(k3)
      )
    ).runSyncUnsafe()

    //then
    r1 shouldBe Some("int")
    r2 shouldBe Some("embstr")
    r3.isDefined shouldBe false
  }

}
