package monix.connect.redis

import monix.connect.redis.client.RedisCmd
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

  it should "bZPopMin" in {

    
  }

  it should "bZPopMax" in {}

  it should "zAdd" in {}

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
