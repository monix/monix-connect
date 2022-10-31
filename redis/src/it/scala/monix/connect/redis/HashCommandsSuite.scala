package monix.connect.redis

import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class HashCommandsSuite
  extends AsyncFlatSpec with MonixTaskTest with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  override implicit val scheduler: Scheduler = Scheduler.io("hash-commands-suite")

  "hDel" should "delete single hash field" in {
    utfConnection.use[Task, Assertion]{ cmd =>
      for {
        k1 <- Task.from(genRedisKey)
        f1 <- Task.from(genRedisKey)
        v <- Task.from(Gen.identifier)
        _ <- cmd.hash.hSet(k1, f1, v)
        existsBefore <- cmd.hash.hExists(k1, f1)
        delete <- cmd.hash.hDel(k1, f1)
        existsAfter <- cmd.hash.hExists(k1, f1)
        deleteNoKey <- cmd.hash.hDel(k1, f1)
      } yield {
        existsBefore shouldBe true
        delete shouldBe true
        existsAfter shouldBe false
        deleteNoKey shouldBe false
      }
    }
  }

  "hDel" should "delete multiple hash fields" in {
    utfConnection.use[Task, Assertion]{ cmd =>
      for {
        k1 <- Task.from(genRedisKey)
        f1 <- Task.from(genRedisKey)
        f2 <- Task.from(genRedisKey)
        f3 <- Task.from(genRedisKey)
        v <- Task.from(Gen.identifier)
        _ <- cmd.hash.hMSet(k1, Map(f1 -> v, f2 -> v, f3 -> v))
        initialLen <- cmd.hash.hLen(k1)
        delete <- cmd.hash.hDel(k1, List(f1, f2))
        finalLen <- cmd.hash.hLen(k1)
        deleteNoKey <- cmd.hash.hDel(k1, List(f1, f2))
      } yield {
        initialLen shouldBe 3L
        delete shouldBe 2L
        finalLen shouldBe 1L
        deleteNoKey shouldBe 0L
      }
    }.assertNoException
  }

  "hExists" should "determine whether a hash field exists or not" in {
    val k: K = genRedisKey.sample.get
    val f: K = genRedisKey.sample.get
    val v: V = genRedisKey.sample.get
    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.hash.hSet(k, f, v)
        exists1 <- cmd.hash.hExists(k, f)
        exists2 <- cmd.hash.hExists(k, "non-existing-field")
      } yield {
        exists1 shouldBe true
        exists2 shouldBe false
      }
    }
  }

  "hGet" should "return an empty value when accessing to a non existing hash" in {
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get

    utfConnection.use(_.hash.hGet(key, field)).asserting(_ shouldBe None)
  }

  "hIncr" should "return None when the hash does not exists " in {
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get

    utfConnection.use(_.hash.hGet(key, field)).asserting(_ shouldBe None)
  }

  "hIncrBy" should "increment the integer value of a hash field by the given number" in {
    val k: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val v: V = "1"

    redis.connectUtf.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.hash.hSet(k, f1, v)
        inc0 <- cmd.hash.hIncrBy(k, f1, 0)
        inc1 <- cmd.hash.hIncrBy(k, f1, 1)
        inc2 <- cmd.hash.hIncrBy(k, f1, 2)
        inc3 <- cmd.hash.hIncrBy(k, f1, 3)
        _ <- cmd.hash.hSet(k, f2, "someString")
        incNonNumber <- cmd.hash.hIncrBy(k, f2, 1).onErrorHandleWith(ex => if (ex.getMessage.contains("not an integer")) Task.now(-10) else Task.now(-11))
        incNotExistingField <- cmd.hash.hIncrBy(k, "none", 1)
        incNotExistingKey <- cmd.hash.hIncrBy("none", "none", 4)
      } yield {
        inc0 shouldBe v.toLong
        inc1 shouldBe 2L
        inc2 shouldBe 4L
        inc3 shouldBe 7L
        incNonNumber shouldBe -10
        incNotExistingField shouldBe 1
        incNotExistingKey shouldBe 4
      }
    }.assertNoException
  }

  it should "increment the float value of a hash field by the given amount " in {
    val k: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val v: V = "1"
    val randomNonExistingKey = genRedisKey.sample.get
    val randomNonExistingField = genRedisKey.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.hash.hSet(k, f1, v)
        inc0 <- cmd.hash.hIncrBy(k, f1, 0.0)
        inc1 <- cmd.hash.hIncrBy(k, f1, 1.1)
        inc2 <- cmd.hash.hIncrBy(k, f1, 2.1)
        inc3 <- cmd.hash.hIncrBy(k, f1, 3.1)
        _ <- cmd.hash.hSet(k, f2, "someString")
        incNonNumber <- cmd.hash.hIncrBy(k, f2, 1.1).onErrorHandleWith(ex => if (ex.getMessage.contains(" not a float")) Task.now(-10.10) else Task.now(-11.11))
        incNotExistingField <- cmd.hash.hIncrBy(k, genRedisKey.sample.get, 1.1)
        incNotExistingKey <- cmd.hash.hIncrBy(randomNonExistingKey, randomNonExistingField, 0.1)
      } yield {
        inc0 shouldBe v.toDouble
        inc1 shouldBe 2.1
        inc2 shouldBe 4.2
        inc3 shouldBe 7.3
        incNonNumber shouldBe -10.10
        incNotExistingField shouldBe 1.1
        incNotExistingKey shouldBe 0.1
      }
    }
  }

  "hGetAll" should "get all the fields and values in a hash" in {
    val k: K = genRedisKey.sample.get
    val mSet = Gen.mapOfN(10, genKv).sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.hash.hMSet(k, mSet)
        getAll <- cmd.hash.hGetAll(k).toListL
        emptyHash <- cmd.hash.hGetAll("nothing").toListL
      } yield {
        getAll should contain theSameElementsAs mSet.toList.map { case (k, v) => (k, v) }
        emptyHash shouldBe List.empty
      }
    }
  }

  "hKeys" should "get all the fields in a hash" in {
    val k: K = genRedisKey.sample.get
    val mSet = Gen.mapOfN(10, genKv).sample.get
    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.hash.hMSet(k, mSet)
        keys <- cmd.hash.hKeys(k).toListL
        emptyKeys <- cmd.hash.hKeys("not exists").toListL
      } yield {
        keys should contain theSameElementsAs mSet.toList.map(_._1)
        emptyKeys shouldBe List.empty
      }
    }
  }

  "hLen" should "set the number of hash fields within a key" in {
    val k1: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val f3: K = genRedisKey.sample.get
    val v: K = genRedisKey.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.hash.hMSet(k1, Map(f1 -> v, f2 -> v, f3 -> v))
        initialLen <- cmd.hash.hLen(k1)
        _ <- cmd.hash.hDel(k1, List(f1, f2, f3))
        finalLen <- cmd.hash.hLen(k1)
        _ <- cmd.hash.hDel(k1, List(f1, f2))
      } yield {
        initialLen shouldBe 3L
        finalLen shouldBe 0L
      }
    }
  }

  "hMSet" should "get the specified hash keys and values" in {
    val k1: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val f3: K = genRedisKey.sample.get
    val v: K = genRedisKey.sample.get

    utfConnection.use { cmd =>
      cmd.hash.hMSet(k1, Map(f1 -> v, f2 -> v)) >>
        cmd.hash.hMGet(k1, f1, f2, f3).toListL
    }.asserting(_ should contain theSameElementsAs List((f1, Some(v)), (f2, Some(v)), (f3, None)))
  }

  "hSet" should "set the string value of a hash field." in {
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get
    val value1: String = genRedisValue.sample.get
    val value2: String = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        setFirst <- cmd.hash.hSet(key, field, value1)
        setSecond <- cmd.hash.hSet(key, field, value2)
        finalValue <- cmd.hash.hGet(key, field)
      } yield {
        setFirst shouldBe true
        setSecond shouldBe false
        finalValue shouldBe Some(value2)
      }
    }
  }

  "hSetNx" should "set the value of a hash field, only if the field does not exist" in {
    val key: K = genRedisKey.sample.get
    val field: K = genRedisKey.sample.get
    val value: V= genRedisValue.sample.get

    utfConnection.use[Task, Assertion](cmd =>
      for {
        firstSet <- cmd.hash.hSetNx(key, field, value)
        secondSet <- cmd.hash.hSetNx(key, field, value)
        get <- cmd.hash.hGet(key, field)
        getNone <- cmd.hash.hGet(key, "none")
      } yield {
        firstSet shouldBe true
        secondSet shouldBe false
        get shouldBe Some(value)
        getNone shouldBe None
      }
    )
  }

  "hStrLen" should "get the string length of the field value" in {
    val k1: K = genRedisKey.sample.get
    val f1: K = genRedisKey.sample.get
    val f2: K = genRedisKey.sample.get
    val v: K = genRedisKey.sample.get

    utfConnection.use[Task, Assertion] {
      cmd =>
        for {
          _ <- cmd.hash.hSet(k1, f1, v)
          f1StrLen <- cmd.hash.hStrLen(k1, f1)
          f2StrLen <- cmd.hash.hStrLen(k1, f2)
          k2StrLen <- cmd.hash.hStrLen("non-existing-key", f1)
        } yield {
          f1StrLen shouldBe v.length
          f2StrLen shouldBe 0L
          k2StrLen shouldBe 0L
        }
    }
  }

  "hVals" should "get all hash values of a key " in {

    utfConnection.use[Task, Assertion] {
      cmd =>
        for {
          k1 <- Task.from(genRedisKey)
          k2 <- Task.from(genRedisKey)
          k3 <- Task.from(genRedisKey)
          f1 <- Task.from(genRedisKey)
          f2 <- Task.from(genRedisKey)
          f3 <- Task.from(genRedisKey)
          v1 <- Task.from(Gen.identifier)
          v2 <- Task.from(Gen.identifier)
          v3 <- Task.from(Gen.identifier)
          _ <- cmd.hash.hMSet(k1, Map(f1 -> v1, f2 -> v2, f3 -> v3))
          k1Vals <- cmd.hash.hVals(k1).toListL
          k2Vals <- cmd.hash.hVals(k2).toListL
        } yield {
          k1Vals should contain theSameElementsAs List(v1, v2, v3)
          k2Vals shouldBe List.empty
        }
    }
  }

}
