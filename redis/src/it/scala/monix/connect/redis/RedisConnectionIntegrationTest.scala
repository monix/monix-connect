package monix.connect.redis

import io.lettuce.core.ScoredValue
import monix.connect.redis.client.{Redis, RedisCmd}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class RedisConnectionIntegrationTest extends AnyFlatSpec
  with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  s"$StringCommands" should "insert a string into the given key and get its size from redis" in {
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

  s"${ServerCommands} " should "insert a string into key and get its size from redis" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use(_.string.set(key, value)).runSyncUnsafe()
    val beforeFlush: Boolean = utfConnection.use(_.key.exists(key)).runSyncUnsafe()

    //and
    utfConnection.use(_.server.flushAll()).runSyncUnsafe()
    val afterFlush: Boolean = utfConnection.use(_.key.exists(key)).runSyncUnsafe()

    //then
    beforeFlush shouldEqual true
    afterFlush shouldEqual false
  }

  s"${KeyCommands}" should "handles ttl correctly" in {
    //given
    val key1: K = genRedisKey.sample.get
    val key2: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    utfConnection.use(_.string.set(key1, value)).runSyncUnsafe()

    //when
    val (initialTtl, expire, finalTtl, existsWithinTtl, existsRenamed, existsAfterFiveSeconds) = {
      utfConnection.use { case RedisCmd(_, keys, _, _, _ ,_, _) =>
        for {
          initialTtl <- keys.ttl(key1)
          expire <- keys.pExpire(key1, 2.seconds)
          finalTtl <- keys.ttl(key1)
          existsWithinTtl <- keys.exists(key1)
          _ <- keys.rename(key1, key2)
          existsRenamed <- keys.exists(key2)
          _ <- Task.sleep(3.seconds)
          existsAfterFiveSeconds <- keys.exists(key2)
        } yield (initialTtl, expire, finalTtl, existsWithinTtl, existsRenamed, existsAfterFiveSeconds)
      }
    }.runSyncUnsafe()

    //then
    (initialTtl.toMillis < 0L) shouldBe true
    (finalTtl.toMillis > 0L) shouldBe true
    expire shouldBe true
    existsWithinTtl shouldBe 1L
    existsRenamed shouldBe 1L
    existsAfterFiveSeconds shouldBe 0L
  }

  s"${ListCommands}" should "insert elements into a list and reading back the same elements" in {
    //given
    val key: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get.map(_.toString)

    //when
    utfConnection.use(_.list.lPush(key, values: _*)).runSyncUnsafe()

    //and
    val l: List[String] = utfConnection.use(_.list.lRange(key, 0, values.size).toListL).runSyncUnsafe()

    //then
    l should contain theSameElementsAs values
  }

  s"$SetCommands" should "allow to compose nice for comprehensions" in {
    //given
    val k1: K = genRedisKey.sample.get
    val m1: List[String] = genRedisValues.sample.get.map(_.toString)
    val k2: K = genRedisKey.sample.get
    val m2: List[String] = genRedisValues.sample.get.map(_.toString)
    val k3: K = genRedisKey.sample.get

    //when
    val (size1, size2, moved) = {
      utfConnection.use { case RedisCmd(_, _, _, _, set ,_, _)  =>
        for {
          size1 <- set.sAdd(k1, m1: _*)
          size2 <- set.sAdd(k2, m2: _*)
          _ <- set.sAdd(k3, m1: _*)
          moved <- set.sMove(k1, k2, m1.head)
        } yield {
          (size1, size2, moved)
        }
      }
    }.runSyncUnsafe()

    //and
    val (s1, s2, union, diff) = {
      utfConnection.use { case RedisCmd(_, _, _, _, set ,_, _)  =>
        for {
          s1    <- set.sMembers(k1).toListL
          s2    <- set.sMembers(k2).toListL
          union <- set.sUnion(k1, k2).toListL
          diff  <- set.sDiff(k3, k1).toListL
        } yield (s1, s2, union, diff)
      }.runSyncUnsafe()
    }

    //then
    size1 shouldBe m1.size
    s1.size shouldEqual (m1.size - 1)
    size2 shouldBe m2.size
    s2.size shouldEqual (m2.size + 1)
    moved shouldBe true
    s1 shouldNot contain theSameElementsAs m1
    s2 shouldNot contain theSameElementsAs m2
    union should contain theSameElementsAs m1 ++ m2
    //although the list are not equal as at the beginning because of the move operation, its union still is the same
    diff should contain theSameElementsAs List(m1.head)
    //the difference between the k3 and k1 is equal to the element that was moved
  }

  s"${SortedSetCommands}" should "insert elements into a with no order and reading back sorted" in {
    //given
    val k: K = genRedisKey.sample.get
    val v0: String = Gen.alphaLowerStr.sample.get
    val v1: String = Gen.alphaLowerStr.sample.get
    val v2: String = Gen.alphaLowerStr.sample.get
    val minScore: Double = 1
    val middleScore: Double = 3
    val maxScore: Double = 4
    val incrby: Double = 2

    //when
    val t: Task[(Option[ScoredValue[String]], Option[ScoredValue[String]])] =
      utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
        for {
          _ <- sortedSet.zAdd(k, minScore, v0)
          _ <- sortedSet.zAdd(k, middleScore, v1)
          _ <- sortedSet.zAdd(k, maxScore, v2)
          _ <- sortedSet.zIncrBy(k, incrby, v1)
          min <- sortedSet.zPopMin(k)
          max <- sortedSet.zPopMax(k)
        } yield (min, max)
      }

    //then
    val (min, max) = t.runSyncUnsafe()
    min.map(_.getScore) shouldBe Some(minScore)
    min.map(_.getValue) shouldBe v0
    max.map(_.getScore) shouldBe middleScore + incrby
    max.map(_.getValue) shouldBe v1
  }

  s"$Redis" should "allow to composition of different Redis submodules" in {
    //given
    val k1: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get.toString
    val k2: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get.map(_.toString)
    val k3: K = genRedisKey.sample.get

    val (v: Option[String], len: Long, list, keys: List[String]) = {
      utfConnection.use { case RedisCmd(_, keys, list, server, _, _, string) =>
        for {
          _ <- server.flushAll()
          _ <- keys.touch(k1)
          _ <- string.set(k1, value)
          _ <- keys.rename(k1, k2)
          _ <- list.lPush(k3, values: _*)
          v <- string.get(k2)
          _ <- v match {
            case Some(value) => list.lPushX(k3, value)
            case None => Task.unit
          }
          _ <- keys.del(k2)
          len <- list.lLen(k3)
          l <- list.lRange(k3, 0, -1).toListL
          keys <- keys.keys("*").toListL // unsafe operation
        } yield (v, len, l, keys)
      }
    }.runSyncUnsafe()

    v shouldBe Some(value)
    len shouldBe values.size + 1
    list should contain theSameElementsAs value :: values
    keys.size shouldBe 1
    keys.head shouldBe k3
  }

}
