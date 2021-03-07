package monix.connect.redis

import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class HashCommandsIntegrationTest
  extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  "hDel" should "deletes single hash field" in {
    //given
    val k1: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val v: K = genRedisKey.sample.get

    utfConnection.use { cmd =>
      //when
      for {
        _ <- cmd.hash.hSet(k1, f1, v)
        existsBefore <- cmd.hash.hExists(k1, f1)
        delete <- cmd.hash.hDel(k1, f1)
        existsAfter <- cmd.hash.hExists(k1, f1)
        deleteNoKey <- cmd.hash.hDel(k1, f1)
      } yield {
        //then
        existsBefore shouldBe true
        delete shouldBe true
        existsAfter shouldBe false
        deleteNoKey shouldBe false
      }
    }.runSyncUnsafe()

  }

  "hDel" should "deletes multiple hash fields" in {
    //given
    val k1: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val f3: K = genRedisKey.sample.get
    val v: K = genRedisKey.sample.get

    utfConnection.use { cmd =>
      //when
      for {
        _ <- cmd.hash.hMSet(k1, Map(f1 -> v, f2 -> v, f3 -> v))
        initialLen <- cmd.hash.hLen(k1)
        delete <- cmd.hash.hDel(k1, List(f1, f2))
        finalLen <- cmd.hash.hLen(k1)
        deleteNoKey <- cmd.hash.hDel(k1, List(f1, f2))
      } yield {
        //then
        initialLen shouldBe 3L
        delete shouldBe 2L
        finalLen shouldBe 1L
        deleteNoKey shouldBe 0L
      }
    }.runSyncUnsafe()

  }

  it should "exists" in {
    //given
    val k: K = genRedisKey.sample.get
    val f: K = genRedisKey.sample.get
    val v: V = genRedisKey.sample.get

    utfConnection.use { cmd =>
      //when
      for {
        _ <- cmd.hash.hSet(k, f, v)
        exists1 <- cmd.hash.hExists(k, f)
        exists2 <- cmd.hash.hExists(k, "non-existing-field")
      } yield {
        //then
        exists1 shouldBe true
        exists2 shouldBe false
      }
    }.runSyncUnsafe()
  }

  "hGet" should "return an empty value when accessing to a non existing hash" in {
    //given
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get

    //when
    val r: Option[String] = utfConnection.use(_.hash.hGet(key, field)).runSyncUnsafe()

    //then
    r shouldBe None
  }

  "hIncr" should "return None when the hash does not exists " in {
    //given
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get

    //when
    val r: Option[String] = utfConnection.use(_.hash.hGet(key, field)).runSyncUnsafe()

    //then
    r shouldBe None
  }

  it should "incr by" in {
    //given
    val k: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val v: V = "1"

    utfConnection.use { cmd =>
      //when
      for {
        _ <- cmd.hash.hSet(k, f1, v)
        inc0 <- cmd.hash.hIncrBy(k, f1, 0)
        inc1 <- cmd.hash.hIncrBy(k, f1, 1)
        inc2 <- cmd.hash.hIncrBy(k, f1, 2)
        inc3 <- cmd.hash.hIncrBy(k, f1, 3)
        _ <- cmd.hash.hSet(k, f2, "someString")
        incNonNumber <- cmd.hash.hIncrBy(k, f2, 1)
        incNotExistingField <- cmd.hash.hIncrBy(k, "none", 1)
        incNotExistingKey <- cmd.hash.hIncrBy("none", "none", 0)
      } yield {
        //then
        inc0 shouldBe v.toLongOption
        inc1 shouldBe Some(2L)
        inc2 shouldBe Some(4L)
        inc3 shouldBe Some(7L)
        incNonNumber shouldBe None
        incNotExistingField shouldBe "1"
        incNotExistingKey shouldBe None
      }
    }.runSyncUnsafe()
  }

  it should "incr by double" in {
    //given
    val k: K = genRedisKey.sample.get
    val f: K = genRedisKey.sample.get
    val v: V = "1"

    utfConnection.use { cmd =>
      //when
      for {
        _ <- cmd.hash.hSet(k, f, v)
        inc0 <- cmd.hash.hIncrBy(k, f, 0.0)
        inc1 <- cmd.hash.hIncrBy(k, f, 1.1)
        inc2 <- cmd.hash.hIncrBy(k, f, 2.1)
        inc3 <- cmd.hash.hIncrBy(k, f, 3.1)
        incNone <- cmd.hash.hIncrBy(k, "none", 0.0)
      } yield {
        //then
        inc0 shouldBe v.toDoubleOption
        inc1 shouldBe Some(2.1)
        inc2 shouldBe Some(4.2)
        inc3 shouldBe Some(7.3)
        incNone shouldBe None
      }
    }.runSyncUnsafe()
  }

  it should "h get all" in {

  }

  it should "h keys" in {

  }

  "hLen" should "het the number of hash fields within a key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val f3: K = genRedisKey.sample.get
    val v: K = genRedisKey.sample.get

    utfConnection.use { cmd =>
      //when
      for {
        _ <- cmd.hash.hMSet(k1, Map(f1 -> v, f2 -> v, f3 -> v))
        initialLen <- cmd.hash.hLen(k1)
        _ <- cmd.hash.hDel(k1, List(f1, f2, f3))
        finalLen <- cmd.hash.hLen(k1)
        _ <- cmd.hash.hDel(k1, List(f1, f2))
      } yield {
        //then
        initialLen shouldBe 3L
        finalLen shouldBe 0L
      }
    }.runSyncUnsafe()

  }

  "hMSet" should "get the specified hash keys and values" in {
    //given
    val k1: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val f3: K = genRedisKey.sample.get
    val v: K = genRedisKey.sample.get

    utfConnection.use { cmd =>
      //when
      for {
        _ <- cmd.hash.hMSet(k1, Map(f1 -> v, f2 -> v))
        kV <- cmd.hash.hMGet(k1, f1, f2, f3).toListL
      } yield {
        //then
        kV should contain theSameElementsAs List((f1, Some(v)), (f2, Some(v)), (f3, None))
      }
    }.runSyncUnsafe()
  }

  it should "hm set" in {

  }

  //todo
  it should "hscan" in {

  }

  it should "hset" in {
    //given
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use(_.hash.hSet(key, field, value)).runSyncUnsafe()

    //and
    val r: Option[String] = utfConnection.use(_.hash.hGet(key, field)).runSyncUnsafe()

    //then
    r shouldBe Some(value)
  }

  //todo test
  it should "hset nx" in {}

  "hStrLen" should "get the string length of the field value" in {
    //given
    val k1: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val v: K = genRedisKey.sample.get

    utfConnection.use {
      cmd =>
        //when
        for {
          _ <- cmd.hash.hSet(k1, f1, v)
          f1StrLen <- cmd.hash.hStrLen(k1, f1)
          f2StrLen <- cmd.hash.hStrLen(k1, f2)
          k2StrLen <- cmd.hash.hStrLen("non-existing-key", f1)
        } yield {
          //then
          f1StrLen shouldBe f1.length
          f2StrLen shouldBe 0L
          k2StrLen shouldBe 0L
        }
    }.runSyncUnsafe()
  }

  "hVals" should "get all hash values of a key " in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    //and
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val f3: K = genRedisKey.sample.get
    //and
    val v1: K = genRedisKey.sample.get
    val v2: K = genRedisKey.sample.get
    val v3: K = genRedisKey.sample.get

    utfConnection.use {
      cmd =>
        //when
        for {
          _ <- cmd.hash.hMSet(k1, Map(f1 -> v1, f2 -> v2, f3 -> v3))
          k1Vals <- cmd.hash.hVals(k1).toListL
          k2Vals <- cmd.hash.hVals(k2).toListL
        } yield {
          //then
          k1Vals should contain theSameElementsAs List(v1, v2, v3)
          k2Vals shouldBe List.empty
        }
    }.runSyncUnsafe()
  }


}
