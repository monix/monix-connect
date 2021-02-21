package monix.connect.redis

import monix.connect.redis.client.RedisCmd
import monix.connect.redis.commands.SortedSetCommands
import monix.connect.redis.domain.VScore
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class SortedSetCommandsSuite
  extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  s"${SortedSetCommands}" should "insert elements with no order and reading back sorted" in {
    //given
    val k: K = genRedisKey.sample.get
    val v0: String = Gen.alphaLowerStr.sample.get
    val v1: String = Gen.alphaLowerStr.sample.get
    val v2: String = Gen.alphaLowerStr.sample.get
    val minScore: Double = 1
    val middleScore: Double = 3
    val maxScore: Double = 4
    val incrby: Double = 2

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k, minScore, v0)
        _ <- sortedSet.zAdd(k, middleScore, v1)
        _ <- sortedSet.zAdd(k, maxScore, v2)
        _ <- sortedSet.zIncrBy(k, incrby, v1)
        min <- sortedSet.zPopMin(k)
        max <- sortedSet.zPopMax(k)
      } yield {
        //then
        min.score shouldBe minScore
        min.value shouldBe Some(v0)
        max.score shouldBe middleScore + incrby
        max.value shouldBe Some(v1)
      }
    }.runSyncUnsafe()

  }

  "bZPopMin" should "remove and return the member with lowest score from one of the keys" in {
    //given
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val vScore1 = genVScore(1).sample.get
    val vScore3 = genVScore(3).sample.get
    val vScore5 = genVScore(5).sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScore1, vScore5)
        _ <- sortedSet.zAdd(k2, vScore3)
        firstMin <- sortedSet.bZPopMin(1.seconds, k1, k2)
        secondMin <- sortedSet.bZPopMin(1.seconds, k1, k2)
        thirdMin <- sortedSet.bZPopMin(1.seconds, k1, k2)
        empty <- sortedSet.bZPopMin(1.seconds, k1, k2)
      } yield {
        //then
        firstMin shouldBe Some(k1, vScore1)
        secondMin shouldBe Some(k1, vScore5) // returns 5 because is the min score in k1
        thirdMin shouldBe Some(k2, vScore3)
        empty shouldBe None
      }
    }.runSyncUnsafe()

  }

  "bZPopMax" should "remove and return the member with lowest score from one of the keys" in {
    //given
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val vScore1 = genVScore(1).sample.get
    val vScore3 = genVScore(3).sample.get
    val vScore5 = genVScore(5).sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScore1, vScore5)
        _ <- sortedSet.zAdd(k2, vScore3)
        firstMax <- sortedSet.bZPopMax(1.seconds, k1, k2)
        secondMax <- sortedSet.bZPopMax(1.seconds, k1, k2)
        thirdMax <- sortedSet.bZPopMax(1.seconds, k1, k2)
        empty <- sortedSet.bZPopMax(1.seconds, k1, k2)
      } yield {
        //then
        firstMax shouldBe Some(k1, vScore5)
        secondMax shouldBe Some(k1, vScore1) // returns 5 because is the min score in k1
        thirdMax shouldBe Some(k2, vScore3)
        empty shouldBe None
      }
    }.runSyncUnsafe()
  }

  "zAdd" should "add a single scored values to the sorted set" in {
    //given
    val k1 = genRedisKey.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAdd(k1, 1, k1)
        add2 <- sortedSet.zAdd(k1, 5, k1)
        size1 <- sortedSet.zCountAll(k1)
        value <- sortedSet.zPopMin(k1)

      } yield {
        //then
        add1 shouldBe true
        add2 shouldBe false //false if the value already existed
        size1 shouldBe 1L
        value.score shouldBe 5
      }
    }.runSyncUnsafe()
  }

  it should "add multiple scored values to the sorted set" in {
    //given
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val vScore1 = genVScore(1).sample.get
    val vScore2 = genVScore(2).sample.get
    val vScore3 = genVScore(3).sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAdd(k1, vScore1)
        add2 <- sortedSet.zAdd(k2, vScore2, vScore3)
        add3 <- sortedSet.zAdd(k2, List(vScore1, vScore2, vScore3))
        size1 <- sortedSet.zCountAll(k1)
        size2 <- sortedSet.zCountAll(k2)
      } yield {
        //then
        add1 shouldBe 1L
        add2 shouldBe 2L
        add3 shouldBe 1L
        size1 shouldBe 1L
        size2 shouldBe 3L
      }
    }.runSyncUnsafe()
  }

  it should "zAddIncr" in {}

  it should "zCard" in {}

  it should "zCount" in {}

  it should "zIncrBy" in {}

  it should "zInterStore" in {}

  it should "zLexCount" in {}

  it should "zPopMin" in {}

  it should "zPopMax" in {}

  it should "zRange" in {}

  it should "zRangeWithScores" in {}

  it should "zRangeByLex" in {}

  it should "zRangeByScore" in {}

  it should "zRangeByScoreWithScores" in {}

  it should "zRank" in {}

  it should "zRem" in {}

  it should "zRemRangeByLex" in {}

  it should "zRemRangeByRank" in {}

  it should "zRemRangeByScore" in {}

  it should "zRevRange" in {}

  it should "zRevRangeWithScores" in {}

  it should "zRevRangeByLex" in {}

  it should "zRevRangeByScore" in {}

  it should "zRevRangeByScoreWithScores" in {}

  it should "zScan" in {}

  it should "zScore" in {}

  it should "zUnionStore" in {}

}
