package monix.connect.redis

import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class StringCommandsSuite
  extends AsyncFlatSpec with MonixTaskSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  override implicit val scheduler: Scheduler = Scheduler.io("string-commands-suite")

  "append" should "append a value to a key" in {
    val k1: K = genRedisKey.sample.get
    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
     for {
     append1 <- cmd.string.append(k1, v1)
     append2 <- cmd.string.append(k1, v2)
     } yield {
       append1 shouldBe v1.length
       append2 shouldBe v1.length + v2.length
     }
    }
  }

  "bitCount" should "count set bits in a string" in {
    val k1: K = genRedisKey.sample.get
    val word: String = "Pau" //01010000 01100001 01110101

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        bitCount0 <- cmd.string.bitCount(k1)
        bitCount1 <- cmd.string.set(k1, word) >> cmd.string.bitCount(k1)
        bitCount2 <- cmd.string.bitCount(k1, 0L, 1L)
      } yield {
        bitCount0 shouldBe 0L
        bitCount1 shouldBe 10L
        bitCount2 shouldBe 5L
      }
    }
  }

  "bitPosOne and bitPosZero" should "find the position of the first bit set or clear in a string respectively" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val word1: String = "you" //01111001 01101111 01110101

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.string.set(k1, word1) >>
          cmd.string.setBit(k2, 0L, 1) >>
          cmd.string.setBit(k2, 1L, 1) >>
          cmd.string.setBit(k2, 2L, 1) >>
          cmd.string.setBit(k2, 3L, 1) >>
          cmd.string.setBit(k2, 4L, 0)
        _ <- cmd.string.setBit(k3, 0L, 0)
        stateBitPos0 <- cmd.string.bitPosOne("no-existing-key")
        stateBitPos1 <- cmd.string.bitPosOne(k1)
        stateBitPos2 <- cmd.string.bitPosOne(k2)
        stateBitPos3 <- cmd.string.bitPosOne(k3)
        noStateBitPos1 <- cmd.string.bitPosZero(k1)
        noStateBitPos2 <- cmd.string.bitPosZero(k2)
        noStateBitPos3 <- cmd.string.bitPosZero(k3)
      } yield {
        stateBitPos0 shouldBe None
        stateBitPos1 shouldBe Some(1)
        stateBitPos2 shouldBe Some(0)
        stateBitPos3 shouldBe None
        noStateBitPos1 shouldBe Some(0)
        noStateBitPos2 shouldBe Some(4)
        noStateBitPos3 shouldBe Some(0)
      }
    }
  }

  "bitOp" should "support (AND, OR, NOT and XOR)" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val bitAndK: K = genRedisKey.sample.get
    val bitOrK: K = genRedisKey.sample.get
    val bitNotK: K = genRedisKey.sample.get
    val bitXorK: K = genRedisKey.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
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
    }
  }

  "decr" should "decrement the integer value of a key by one" in {
    val k1: K = genRedisKey.sample.get
    val v1: Int = Gen.choose(50, 100).sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.string.append(k1, v1.toString)
        decr <- cmd.string.decr(k1)
      } yield {
        decr shouldBe Some(v1 - 1)
      }
    }
  }

  "decrBy" should "decrement the integer value of a key by the given number" in {
    val k1: K = genRedisKey.sample.get
    val n: Int = Gen.choose(0, 20).sample.get
    val v1: Int = Gen.choose(n + 1, 100).sample.get

    utfConnection.use { cmd =>
      cmd.string.append(k1, v1.toString) *>
        cmd.string.decrBy(k1, n)
    }.asserting(_ shouldBe Some(v1 - n))
  }

  "set and get" should "set and then get the value of a key respectively" in {
    val k: K = genRedisKey.sample.get
    val v: V = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.string.set(k, v)
        value <- cmd.string.get(k)
        nonExists <- cmd.string.get("non-existing-key")
      } yield {
        value shouldBe Some(v)
        nonExists shouldBe None
      }
    }
  }

  "setBit and getBit" should "set and get the values of a key respectively" in {
    val k: K = genRedisKey.sample.get

    utfConnection.use[Task, Assertion]  { cmd =>
      for {
        _ <- cmd.string.setBit(k, 0, 1) >> cmd.string.setBit(k, 1, 0) >> cmd.string.setBit(k, 2, 1)
        value <- Task.parZip3(cmd.string.getBit(k, 0), cmd.string.getBit(k, 1), cmd.string.getBit(k, 2))
        nonExists <- cmd.string.get("non-existing-key")
      } yield {
        value shouldBe (Some(1), Some(0), Some(1))
        nonExists shouldBe None
      }
    }
  }

  "getRange" should "get a substring of the string stored at a key" in {
    val k: K = genRedisKey.sample.get
    val v: V = "thisIsARandomString"

    utfConnection.use[Task, Assertion] { cmd =>
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
    }
  }

  "getSet" should "set the string value of a key and return its old value." in {
    val k: K = genRedisKey.sample.get
    val initialValue: V = genRedisValue.sample.get
    val finalValue: V = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        initialGetSet <- cmd.string.getSet(k, initialValue)
        finalGetSet <- cmd.string.getSet(k, finalValue)
      } yield {
        initialGetSet shouldBe None
        finalGetSet shouldBe Some(initialValue)
      }
    }
  }

  "incr" should "increment the integer value of a key by one." in {
    val k1: K = genRedisKey.sample.get
    val v1: Int = Gen.choose(50, 100).sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.string.append(k1, v1.toString)
        incr <- cmd.string.incr(k1)
      } yield {
        incr shouldBe Some(v1 + 1)
      }
    }
  }

  "incrBy" should "increment the integer value of a key by the given amount" in {
    val k1: K = genRedisKey.sample.get
    val n: Int = Gen.choose(0, 20).sample.get
    val v1: Int = Gen.choose(n + 1, 100).sample.get

    utfConnection.use { cmd =>
      cmd.string.append(k1, v1.toString) *>
        cmd.string.incrBy(k1, n)
    }.asserting(_ shouldBe Some(v1 + n))
  }

  "incrByFloat" should "increment the float value of a key by the given amount" in {
    val k1: K = genRedisKey.sample.get
    val n: Double = BigDecimal(Gen.choose(0, 20).sample.get).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val v1: Double = BigDecimal(Gen.choose(n + 1, 100).sample.get).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    utfConnection.use { cmd =>
      cmd.string.append(k1, v1.toString) *> cmd.string.incrByFloat(k1, n)
      }.asserting(_ shouldBe Some(v1 + n))
  }

  "mGet" should "get the values of all the given keys" in {
    val kV: List[(K, Option[V])] = List.fill(10)(genRedisKey, genRedisValue).map{ case (k, v) => (k.sample.get, Some(v.sample.get)) }
    val nonExistingKey = genRedisKey.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- Task.traverse(kV){ case (k, v) => cmd.string.set(k, v.get) }
        mGet <- cmd.string.mGet(kV.map(_._1): _*).toListL
        mGetL <- cmd.string.mGet(kV.map(_._1):+nonExistingKey).toListL
      } yield {
        mGet should contain theSameElementsAs kV
        mGetL should contain theSameElementsAs kV:+(nonExistingKey, None)
      }
    }
  }

  "mSet" should "set multiple keys to multiple values" in {
    val kV: Map[K, V] = List.fill(10)(genRedisKey, genRedisValue).map{ case (k, v) => (k.sample.get, v.sample.get) }.toMap

    utfConnection.use { cmd =>
      cmd.string.mSet(kV) *>
        cmd.string.mGet(kV.keys.toList).toListL
    }.asserting(_ should contain theSameElementsAs kV.mapValues(Some(_)).toList)
  }

  "mSetNx" should "set multiple keys to multiple values, only if none of the keys exist" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val k3: K = genRedisKey.sample.get
    val k4: K = genRedisKey.sample.get
    val map1: Map[K, V] = Map(k1 -> "1", k2 -> "2", k3 -> "3a")
    val map2: Map[K, V] = Map(k3 -> "3b", k4 -> "4")

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        mSet1 <- cmd.string.mSetNx(map1)
        mSet2 <- cmd.string.mSetNx(map2)
        result <- cmd.string.mGet(k1, k2, k3, k4).toListL
      } yield {
        mSet1 shouldBe true
        mSet2 shouldBe false
        result should contain theSameElementsAs Map(k1 -> Some("1"), k2 -> Some("2"), k3 -> Some("3a"), k4 -> None).toList
      }
    }
  }

  "setEx" should "set the value and expiration of a key" in {
    val k1: K = genRedisKey.sample.get
    val v1: V = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.string.setEx(k1, 1.seconds, v1)
        ttl <- cmd.key.pttl(k1)
        existsAfterTimeout <- Task.sleep(2.seconds) >> cmd.key.exists(k1)
      } yield {
        ttl.toSeconds should be < 2L
        ttl.toMillis should be > 0L
        existsAfterTimeout shouldBe false
      }
    }
  }

  "setNx" should "set the value of a key, only if the key does not exist" in {
    val k1: K = genRedisKey.sample.get
    val v1: V = genRedisValue.sample.get
    val v2: V = genRedisValue.sample.get

    utfConnection.use { cmd =>
      for {
        set1 <- cmd.string.setNx(k1, v1)
        set2 <- cmd.string.setNx(k1, v2)
        result <- cmd.string.get(k1)
      } yield {
        set1 shouldBe true
        set2 shouldBe false
        result shouldBe Some(v1)
      }
    }.runSyncUnsafe()
  }

  "setRange" should "overwrite part of a string at key starting at the specified offset" in {
    val k1: K = genRedisKey.sample.get
    val v1: V = "thisIsSetRangeTest"
    val expected: V = "thisIsSetRangeExample"

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        set1 <- cmd.string.setRange(k1, 1L, v1)
        set2 <- cmd.string.setRange(k1, 15L, "Example")
        result <- cmd.string.get(k1)
      } yield {
        set1 shouldBe v1.length + 1L
        set2 shouldBe expected.length + 1L
        //`\u0000` is present since we used an offset of 1L
        result shouldBe Some("\u0000" + expected)
      }
    }
  }

  "strLen" should "get the length of the value stored in a key" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val v1: V = genRedisValue.sample.get
    val v2: V = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.string.set(k1, v1) >> cmd.string.set(k2, v2)
        strLen1 <- cmd.string.strLen(k1)
        strLen2 <- cmd.string.strLen(k2)
        strLenEmpty <- cmd.string.strLen("non-existing-key")

      } yield {
        strLen1 shouldBe v1.length
        strLen2 shouldBe v2.length
        strLenEmpty shouldBe 0L
      }
    }
  }

}
