package monix.connect.redis

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class StringCommandsSuite
  extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  it should "insert a string into the given key and get its size from redis" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use(_.string.set(key, value)).runSyncUnsafe()

    //and
    val t: Task[Long] = utfConnection.use(_.string.strLen(key))

    //then
    val lenght: Long = t.runSyncUnsafe()
    lenght shouldBe value.length
  }

  "append" should "append a value to a key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
     for {
     append1 <- cmd.string.append(k1, v1)
     append2 <- cmd.string.append(k1, v2)
     } yield {
       append1 shouldBe v1.length
       append2 shouldBe v1.length + v2.length
     }
    }.runSyncUnsafe()
  }

  it should "bitCount" in {
    //given
    val k1: K = genRedisKey.sample.get
    val word: String = "Pau" //01010000 01100001 01110101

    //when
    utfConnection.use { cmd =>
      for {
        bitCount0 <- cmd.string.bitCount(k1)
        bitCount1 <- cmd.string.set(k1, word) >> cmd.string.bitCount(k1)
        bitCount2 <- cmd.string.bitCount(k1, 0L, 1L)
      } yield {
        bitCount0 shouldBe 0L
        bitCount1 shouldBe 10L
        bitCount2 shouldBe 5L
      }
    }.runSyncUnsafe()
  }

  "bitPos" should "bitPos" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val word1: String = "you" //01111001 01101111 01110101

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.set(k1, word1) >>
          cmd.string.setBit(k2, 0L, 1) >>
          cmd.string.setBit(k2, 1L, 1) >>
          cmd.string.setBit(k2, 2L, 1) >>
          cmd.string.setBit(k2, 3L, 1) >>
          cmd.string.setBit(k2, 4L, 0)
        stateBitPos0 <- cmd.string.bitPosOne("no-existing-key")
        stateBitPos1 <- cmd.string.bitPosOne(k1)
        stateBitPos2 <- cmd.string.bitPosOne(k2)
        noStateBitPos1 <- cmd.string.bitPosZero(k1)
        noStateBitPos2 <- cmd.string.bitPosZero(k2)
      } yield {
        stateBitPos0 shouldBe Some(-1)
        stateBitPos1 shouldBe Some(1L)
        stateBitPos2 shouldBe Some(0L)
        noStateBitPos1 shouldBe Some(0L)
        noStateBitPos2 shouldBe Some(4L)
      }
    }.runSyncUnsafe()
  }

  "bitOp" should "support (AND, OR, NOT and XOR)" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val bitAndK: K = genRedisKey.sample.get
    val bitOrK: K = genRedisKey.sample.get
    val bitNotK: K = genRedisKey.sample.get
    val bitXorK: K = genRedisKey.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.setBit(k1, 0L, 0) >>
          cmd.string.setBit(k1, 1L, 1)
       _ <- cmd.string.setBit(k2, 0L, 1) >>
          cmd.string.setBit(k2, 1L, 1)
        and <- cmd.string.bitOpAnd(bitAndK, k1, k2) >>
          Task.parZip2(cmd.string.getBit(bitAndK, 0L), cmd.string.getBit(bitAndK, 1L))
        or <- cmd.string.bitOpOr(bitOrK, k1, k2) >>
          Task.parZip2(cmd.string.getBit(bitOrK, 0L), cmd.string.getBit(bitOrK, 1L))
        not <- cmd.string.bitOpNot(bitNotK, k1) >>
          Task.parZip2(cmd.string.getBit(bitNotK, 0L), cmd.string.getBit(bitNotK, 1L))
        xor <- cmd.string.bitOpXor(bitXorK, k1, k2) >>
          Task.parZip2(cmd.string.getBit(bitXorK, 0L), cmd.string.getBit(bitXorK, 1L))
      } yield {
        and shouldBe (Some(0L), Some(1L))
        not shouldBe (Some(1L), Some(0L))
        or shouldBe (Some(1L), Some(1L))
        xor shouldBe (Some(1L), Some(0L))
      }
    }.runSyncUnsafe()
  }

  "decr" should "decrement the integer value of a key by one" in {
    //given
    val k1: K = genRedisKey.sample.get
    val v1: Int = Gen.choose(50, 100).sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.append(k1, v1.toString)
        decr <- cmd.string.decr(k1)
      } yield {
        decr shouldBe Some(v1 - 1)
      }
    }.runSyncUnsafe()
  }

  "decrBy" should "decrement the integer value of a key by the given number" in {
    //given
    val k1: K = genRedisKey.sample.get
    val n: Int = Gen.choose(0, 20).sample.get
    val v1: Int = Gen.choose(n + 1, 100).sample.get
    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.append(k1, v1.toString)
        decrBy <- cmd.string.decrBy(k1, n)
      } yield {
        decrBy shouldBe Some(v1 - n)
      }
    }.runSyncUnsafe()
  }

  "get" should "get the value of a key" in {
    //given
    val k: K = genRedisKey.sample.get
    val v: V = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.set(k, v)
        value <- cmd.string.get(k)
        nonExists <- cmd.string.get("non-existing-key")
      } yield {
        value shouldBe Some(v)
        nonExists shouldBe None
      }
    }.runSyncUnsafe()
  }

  "getBit" should "getBit" in {
    //given
    val k: K = genRedisKey.sample.get
    val v: V = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.setBit(k, 0, 1) >> cmd.string.setBit(k, 1, 0) >> cmd.string.setBit(k, 2, 1)
        value <- Task.parZip3(cmd.string.getBit(k, 0), cmd.string.getBit(k, 1), cmd.string.getBit(k, 2))
        nonExists <- cmd.string.get("non-existing-key")
      } yield {
        value shouldBe (Some(1), Some(0), Some(1))
        nonExists shouldBe None
      }
    }.runSyncUnsafe()
  }

  "getRange" should "get a substring of the string stored at a key" in {
    //given
    val k: K = genRedisKey.sample.get
    val v: V = "thisIsARandomString"

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.set(k, v)
        range1 <- cmd.string.getRange(k, 0, 5)
        range2 <- cmd.string.getRange(k, 6, 30)
        nonExists <- cmd.string.getRange("non-existing-key", 0, 1)
      } yield {
        range1 shouldBe Some("thisIs")
        range2 shouldBe Some("ARandomString")
        nonExists shouldBe Some("")
      }
    }.runSyncUnsafe()
  }

  "getSet" should "set the string value of a key and return its old value." in {
    //given
    val k: K = genRedisKey.sample.get
    val initialValue: V = genRedisValue.sample.get
    val finalValue: V = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        initialGetSet <- cmd.string.getSet(k, initialValue)
        finalGetSet <- cmd.string.getSet(k, finalValue)
      } yield {
        initialGetSet shouldBe None
        finalGetSet shouldBe Some(initialValue)
      }
    }.runSyncUnsafe()
  }

  "incr" should "increment the integer value of a key by one." in {
    //given
    val k1: K = genRedisKey.sample.get
    val v1: Int = Gen.choose(50, 100).sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.append(k1, v1.toString)
        incr <- cmd.string.incr(k1)
      } yield {
        incr shouldBe Some(v1 + 1)
      }
    }.runSyncUnsafe()
  }

  "incrBy" should "increment the integer value of a key by the given amount" in {
    //given
    val k1: K = genRedisKey.sample.get
    val n: Int = Gen.choose(0, 20).sample.get
    val v1: Int = Gen.choose(n + 1, 100).sample.get
    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.append(k1, v1.toString)
        incrBy <- cmd.string.incrBy(k1, n)
      } yield {
        incrBy shouldBe Some(v1 + n)
      }
    }.runSyncUnsafe()
  }

  "incrByFloat" should "increment the float value of a key by the given amount" in {
    //given
    val k1: K = genRedisKey.sample.get
    val n: Double = Gen.choose(0, 20).sample.get
    val v1: Double = Gen.choose(n + 1, 100).sample.get
    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.string.append(k1, v1.toString)
        incrBy <- cmd.string.incrByFloat(k1, n)
      } yield {
        incrBy shouldBe Some(v1 + n)
      }
    }.runSyncUnsafe()
  }

  "mGet" should "get the values of all the given keys" in {
    //given
    val kV: List[(K, Option[V])] = List.fill(10)(genRedisKey, genRedisValue).map{ case (k, v) => (k.sample.get, Some(v.sample.get)) }
    val nonExistingKey = genRedisKey.sample.get
    //when
    utfConnection.use { cmd =>
      for {
        _ <- Task.traverse(kV){ case (k, v) => cmd.string.set(k, v.get) }
        mGet <- cmd.string.mGet(kV.map(_._1): _*).toListL
        mGetL <- cmd.string.mGet(kV.map(_._1):+nonExistingKey).toListL
      } yield {
        mGet should contain theSameElementsAs kV
        mGetL should contain theSameElementsAs kV:+(nonExistingKey, None)
      }
    }.runSyncUnsafe()
  }

  "mSet" should "set multiple keys to multiple values" in {}

  "mSetNx" should "set multiple keys to multiple values, only if none of the keys exist" in {}

  "set" should "set the string value of a key." in {}

  "setBit" should "sets or clears the bit at offset in the string value stored at key" in {}

  "setEx" should "set the value and expiration of a key" in {}

  "pSetEx" should "set the value and expiration in milliseconds of a key." in {}

  "setNx" should "set the value of a key, only if the key does not exist" in {}

  "setRange" should "overwrite part of a string at key starting at the specified offset" in {}

  "strLen" should "get the length of the value stored in a key" in {}

}
