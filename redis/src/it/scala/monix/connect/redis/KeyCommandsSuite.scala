package monix.connect.redis

import monix.connect.redis.client.{RedisCmd, RedisConnection, RedisUri}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class KeyCommandsSuite
  extends AsyncFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll).runSyncUnsafe()
  }

  "del" should "delete key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    utfConnection.use(cmd => cmd.string.set(k1, value) *> cmd.string.set(k2, value)).runSyncUnsafe()

    utfConnection.use { cmd =>
      //when
      for {
        d1 <- cmd.key.del(k1, k2)
        d2 <- cmd.key.del(k3)
      } yield {
        //then
        d1 shouldBe 2L
        d2 shouldBe 0L
      }
    }.runToFuture

  }

  "unLink" should "unlink one or more keys " in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.set(k1, value) *> cmd.string.set(k2, value)
        r1 <- cmd.key.unLink(k1, k2)
        r2 <- cmd.key.unLink("non-existing-key")
      } yield {
        //then
        r1 shouldBe 2L
        r2 shouldBe 0L
      }
    }.runToFuture
  }

  "dump" should "dump the serialized value stored at the key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.set(k1, value)
        d1 <- cmd.key.dump(k1)
        d2 <- cmd.key.dump(k2)
      } yield {
        //then
        d1 should not be empty
        d2 shouldBe empty
      }
    }.runToFuture
  }

  "exists" should "count the number of existing keys" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.set(k1, value) *> cmd.string.set(k2, value)
        r1 <- cmd.key.exists(List(k1, k2))
        r2 <- cmd.key.exists(List(k3))
      } yield {
        //then
        r1 shouldBe 2L
        r2 shouldBe 0L
      }
    }.runSyncUnsafe()
  }

  it should "assert whether the key exists or not" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.set(k1, value)
        r1 <- cmd.key.exists(k1)
        r2 <- cmd.key.exists(k2)
      } yield {
        //then
        r1 shouldBe true
        r2 shouldBe false
      }
    }.runToFuture
  }

  "expire" should "allow overriding the ttl expiration" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    utfConnection.use(cmd => cmd.string.set(k1, value)).runSyncUnsafe()

    utfConnection.use { cmd =>
      //when
      for {
        r1 <- cmd.key.expire(k1, 9.seconds)
        initialTtl <- cmd.key.pttl(k1)
        isOverwritten <- cmd.key.expire(k1, 15.seconds)
        overwrittenTtl <- cmd.key.pttl(k1)
        expireOnNonExistingKey <- cmd.key.expire(k2, 1002.seconds)
      } yield {
        //then
        r1 shouldBe true
        initialTtl.toSeconds should be < 10L
        isOverwritten shouldBe true
        overwrittenTtl.toSeconds should be > 10L
        expireOnNonExistingKey shouldBe false

      }
    }.runSyncUnsafe()
  }

  it should "allow set long expiration times" in {
    //given
    val k1: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    utfConnection.use(cmd => cmd.string.set(k1, value)).runSyncUnsafe()

    utfConnection.use { cmd =>
      //when
      for {
        r1 <- cmd.key.expire(k1, 99999.days)
        ttl <- cmd.key.pttl(k1)
      } yield {
        //then
        r1 shouldBe true
        ttl.toDays should be <= 99999.days.toDays
      }
    }.runSyncUnsafe()
  }

  "pttl" should "be propagated if key gets renamed" in {
    //given
    val key1: K = genRedisKey.sample.get
    val key2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    utfConnection.use(_.string.set(key1, value)).runSyncUnsafe()

    //when
    utfConnection.use { case RedisCmd(_, keys, _, _, _, _, _) =>
      for {
        ttlEmptyKey <- keys.pttl("non-existing-key")
        initialTtl <- keys.pttl(key1)
        expire <- keys.expire(key1, 2.seconds)
        finalTtl <- keys.pttl(key1)
        existsWithinTtl <- keys.exists(key1)
        _ <- keys.rename(key1, key2) // the ttl should be preserved on the new key
        existsRenamed <- keys.exists(key2)
        _ <- Task.sleep(3.seconds)
        existsAfterFiveSeconds <- keys.exists(key2) //the new key will gone after the ttl.
      } yield {
        //then
        ttlEmptyKey.toMillis shouldBe -2L
        (initialTtl.toMillis < 0L) shouldBe true
        expire shouldBe true
        (2.seconds.toMillis > finalTtl.toMillis) shouldBe true
        (finalTtl.toMillis > 0L) shouldBe true
        existsWithinTtl shouldBe true
        existsRenamed shouldBe true
        existsAfterFiveSeconds shouldBe false
      }
    }.runSyncUnsafe()
  }

  "keys" should "get only matched keys" in {
    //given
    val prefix = "prefix_"
    val k1: K = prefix + genRedisKey.sample.get
    val k2: K = prefix + genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    utfConnection.use { cmd =>
      for {
        //when
        _ <- cmd.string.set(k1, value) *> cmd.string.set(k2, value) *> cmd.string.set(k3, value)
        l <- cmd.key.keys(s"$prefix*").toListL
      } yield {
        //then
        List(k1, k2) should contain theSameElementsAs l
      }
    }.runSyncUnsafe()

  }

  // Todo - Y not supported
  /*"migrate" should "transfer a key from a redis instance to another one" in {
    //given
    val k1: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    val host = "127.0.0.1"
    val port = 7000

    //when
    utfConnection.use{ cmd =>
      cmd.string.set(k1, value) *>
        cmd.key.migrate(host, port, k1, 0, 10.seconds)
    }.runSyncUnsafe()

    //then
    RedisConnection.standalone(RedisUri(host, port)).connectUtf.use(cmd =>
      for {
        exists <- cmd.key.exists(k1)
        get <- cmd.string.get(k1)

      } yield {
        exists shouldBe true
        get shouldBe value
      })
      .runSyncUnsafe()
  }*/

  "move" should "move a key to another database" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    utfConnection.use { cmd => cmd.string.set(k1, value) }.runSyncUnsafe()

    //when
    val (moved1, moved2) =
      utfConnection.use(cmd => Task.parZip2(cmd.key.move(k1, 2), cmd.key.move(k2, 2))).runSyncUnsafe()

    //then
    moved1 shouldBe true
    moved2 shouldBe false // the key did not existed
  }

  "objectEncoding" should "if exists, show the internal encoding representation " in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val v1: V = genRedisValue.sample.get
    val v2: String = "Sample String"

    utfConnection.use(cmd => cmd.string.set(k1, v1) >> cmd.string.set(k2, v2)).runSyncUnsafe()

    val (r1, r2, r3) = utfConnection
      .use(cmd => Task.parZip3(cmd.key.objectEncoding(k1), cmd.key.objectEncoding(k2), cmd.key.objectEncoding(k3)))
      .runSyncUnsafe()

    //then
    r1 shouldBe Some("int")
    r2 shouldBe Some("embstr")
    r3.isDefined shouldBe false
  }

  "touch and objectIdleTime" should "touch and check the idle time of a key respectively" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    utfConnection
      .use(cmd =>
        for {
          //when
          _ <- cmd.string.set(k1, value)
          _ <- cmd.key.touch(k1)
          r1 <- cmd.key.objectIdleTime(k1)
          r2 <- cmd.key.objectIdleTime(k1).delayExecution(4.seconds)
          r3 <- cmd.key.objectIdleTime(k2)
        } yield {
          //then
          r1.isDefined shouldBe true
          (r1.get < 1.second) shouldBe true
          //and
          r2.isDefined shouldBe true
          (r2.get > 2.seconds) shouldBe true
          //and
          r3.isDefined shouldBe false
        })
      .runSyncUnsafe()

  }

  "persist" should "persist a key that has an associated timeout" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    utfConnection
      .use(cmd =>
        //when
        for {
          _ <- cmd.string.set(k1, value) >> cmd.key.expire(k1, 100.seconds) >> cmd.string.set(k2, value)
          r1 <- cmd.key.persist(k1)
          r2 <- cmd.key.persist(k2)
          r3 <- cmd.key.persist(k3)
        } yield {
          //then
          r1 shouldBe true
          r2 shouldBe false // the key did not have associated timeout
          r3 shouldBe false // the key did not existed
        })
      .runSyncUnsafe()

  }

  "randomKey" should "return a random key from the database" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection
      .use(cmd =>
        for {
          _ <- cmd.server.flushAll
          r1 <- cmd.key.randomKey()
          _ <- cmd.string.set(k1, value) >> cmd.string.set(k2, value)
          r2 <- cmd.key.randomKey()
        } yield {
          //then
          r1 shouldBe None
          List(k1, k2).contains(r2.get) shouldBe true
        })
      .runSyncUnsafe()

  }

  "rename" should "rename a key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    utfConnection
      .use(cmd =>
        //when
        for {
          _ <- cmd.string.set(k1, value)
          beforeRename <- Task.parZip2(cmd.key.exists(k1), cmd.key.exists(k2))
          _ <- cmd.key.rename(k1, k2)
          afterRename <- Task.parZip2(cmd.key.exists(k1), cmd.key.exists(k2))
          _ <- cmd.string.set(k3, value)
          _ <- cmd.key.rename(k2, k3) // renames even if new key exists
          afterRenameOnExistingKey <- Task.parZip3(cmd.key.exists(k1), cmd.key.exists(k2), cmd.key.exists(k3))
        } yield {
          //then
          beforeRename shouldBe(true, false)
          afterRename shouldBe(false, true)
          afterRenameOnExistingKey shouldBe(false, false, true)
        })
      .runSyncUnsafe()
  }

  "renameNx" should "rename into a new key only if that did not existed" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    utfConnection
      .use(cmd =>
        //when
        for {
          _ <- cmd.string.set(k1, value)
          beforeRename <- Task.parZip2(cmd.key.exists(k1), cmd.key.exists(k2))
          r1 <- cmd.key.renameNx(k1, k2) // true if new key does not exists
          afterRename <- Task.parZip2(cmd.key.exists(k1), cmd.key.exists(k2))
          _ <- cmd.string.set(k3, value)
          r2 <- cmd.key.renameNx(k2, k3) // false if new key exists
          afterRenameOnExistingKey <- Task.parZip3(cmd.key.exists(k1), cmd.key.exists(k2), cmd.key.exists(k3))
        } yield {
          //then
          r1 shouldBe true
          r2 shouldBe false
          //and
          beforeRename shouldBe(true, false)
          afterRename shouldBe(false, true)
          afterRenameOnExistingKey shouldBe(false, true, true)
        })
      .runSyncUnsafe()
  }

  "restore" should "restore a key with its serialized value" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection
      .use(cmd =>
        for {
          _ <- cmd.string.set(k1, value)
          dump <- cmd.key.dump(k1)
          _ <- cmd.key.restore(k2, 100.seconds, dump)
          v1 <- cmd.string.get(k1)
          v2 <- cmd.string.get(k2)
        } yield {
          //then
          dump should not be empty
          v1 shouldBe v2
        })
      .runSyncUnsafe()
  }

  "sort" should "sort the elements of a list" in {
    //given
    val k1: K = genRedisKey.sample.get
    val v1: String = "1"
    val v2: String = "2"
    val v3: String = "3"
    val v4: String = "4"
    val v5: String = "5"

    //when
    utfConnection
      .use(cmd =>
        for {
          _ <- cmd.list.lPush(k1, v2, v5, v3, v4, v1)
          sorted <- cmd.key.sort(k1).toListL
        } yield {
          //then
          sorted.head shouldBe v1
          sorted.last shouldBe v5
          sorted shouldBe List(v1, v2, v3, v4, v5)
        })
      .runSyncUnsafe()
  }

  "keyType" should "determine the type stored at key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val v1: V = genRedisValue.sample.get
    val v2: V = genRedisValue.sample.get

    //when
    val (strType, listType) = utfConnection
      .use(cmd =>
        for {
          _ <- cmd.string.set(k1, v1)
          kv1Type <- cmd.key.keyType(k1)
          _ <- cmd.list.lPush(k2, v2)
          kv2Type <- cmd.key.keyType(k2)
        } yield (kv1Type, kv2Type)
      )
      .runSyncUnsafe()

    //then
    strType shouldBe Some("string")
    listType shouldBe Some("list")
  }

}
