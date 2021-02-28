package monix.connect.redis

import monix.connect.redis.client.RedisCmd
import monix.connect.redis.commands.SortedSetCommands
import monix.connect.redis.domain.{VScore, ZArgs, ZRange}
import monix.eval.Task
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
        size1 <- sortedSet.zCard(k1)
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

  it should "add multiple scored values to the sorted set and count only the new inserted ones, not the updates" in {
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
        size1 <- sortedSet.zCard(k1)
        size2 <- sortedSet.zCard(k2)
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

  "zAddNx" should "adds one member to the set and never update the score if it already existed" in {
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        addFirst <- sortedSet.zAdd(k1, ZArgs.NX, 1, vA)
        addSameScore <- sortedSet.zAdd(k1, ZArgs.NX, 1, vA)
        elems1 <- sortedSet.zScanVScores(k1).toListL
        addDifScore <- sortedSet.zAdd(k1, ZArgs.NX, 2, vA)
        elems2 <- sortedSet.zScanVScores(k1).toListL
        addDifVal <- sortedSet.zAdd(k1, ZArgs.NX, 1, vB)
        elems3 <- sortedSet.zScanVScores(k1).toListL

      } yield {
        //then
        addFirst shouldBe 1L
        addSameScore shouldBe 0L
        elems1 shouldBe List(VScore(1, vA))
        addDifScore shouldBe 0L
        elems2 shouldBe List(VScore(1, vA))
        addDifVal shouldBe 1L
        elems3 should contain theSameElementsAs List(VScore(1, vA), VScore(1, vB))
      }
    }.runSyncUnsafe()
  }

  it should "add multiple members to the set but never update the score if it already existed" in {
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get
    val vC = genRedisValue.sample.get
    val vScoreA1 = VScore(1, vA)
    val vScoreA2 = VScore(2, vA)
    val vScoreB1 = VScore(3, vB)
    val vScoreC1 = VScore(4, vC)

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAdd(k1, ZArgs.NX, vScoreA1)
        add2 <- sortedSet.zAdd(k1, ZArgs.NX, List(vScoreA2, vScoreB1, vScoreC1))
        elems <- sortedSet.zScanVScores(k1).toListL
      } yield {
        //then
        add1 shouldBe 1L
        add2 shouldBe 2L
        elems should contain theSameElementsAs List(vScoreA1, vScoreB1, vScoreC1)
      }
    }.runSyncUnsafe()
  }

  "zAddCh" should "add multiple members to the set and count all that changed" in {
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get
    val vC = genRedisValue.sample.get
    val vScoreA2 = VScore(2, vA)
    val vScoreB1 = VScore(3, vB)
    val vScoreC1 = VScore(4, vC)

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAdd(k1, ZArgs.CH, 1, vA)
        add2 <- sortedSet.zAdd(k1, ZArgs.CH, List(vScoreA2, vScoreB1, vScoreC1))
        add3 <- sortedSet.zAdd(k1, ZArgs.CH, 2, vA)
        elems <- sortedSet.zScanVScores(k1).toListL
      } yield {
        //then
        add1 shouldBe 1L
        add2 shouldBe 3L
        add3 shouldBe 0L
        elems should contain theSameElementsAs List(vScoreA2, vScoreB1, vScoreC1)
      }
    }.runSyncUnsafe()
  }

  "zAddXx" should "only update members, never add new ones" in {
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get
    val vC = genRedisValue.sample.get
    val vScoreA1 = VScore(1, vA)
    val vScoreA2 = VScore(2, vA)
    val vScoreA3 = VScore(3, vA)
    val vScoreB1 = VScore(1, vB)
    val vScoreB2 = VScore(2, vB)
    val vScoreC1 = VScore(1, vC)

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAdd(k1, ZArgs.XX, 1, vA)
        elems1 <- sortedSet.zScanVScores(k1).toListL
        _ <- sortedSet.zAdd(k1, vScoreA1, vScoreB1)
        add2 <- sortedSet.zAdd(k1, ZArgs.XX, vScoreA2)
        elems2 <- sortedSet.zScanVScores(k1).toListL
        add3 <- sortedSet.zAdd(k1, ZArgs.XX, vScoreA3, vScoreB2, vScoreC1)
        elems3 <- sortedSet.zScanVScores(k1).toListL
      } yield {
        //then
        add1 shouldBe 0L
        elems1 shouldBe List.empty
        add2 shouldBe 0L
        elems2 should contain theSameElementsAs List(vScoreA2, vScoreB1)
        add3 shouldBe 0L
        elems3 should contain theSameElementsAs List(vScoreA3, vScoreB2)
      }
    }.runSyncUnsafe()
  }

  "zAddIncr" should "increments the score by the given number" in {
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAddIncr(k1, 1, vA)
        elems1 <- sortedSet.zScanVScores(k1).toListL
        add2 <- sortedSet.zAddIncr(k1, 3, vA)
        elems2 <- sortedSet.zScanVScores(k1).toListL

      } yield {
        //then
        add1 shouldBe 1L
        elems1 should contain theSameElementsAs List(VScore(1, vA))
        add2 shouldBe 4L
        elems2 should contain theSameElementsAs List(VScore(4, vA))
      }
    }.runSyncUnsafe()
  }

  it should "not increment the score if it already exists" in {
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAddIncr(k1, ZArgs.NX, 1, vA)
        elems1 <- sortedSet.zScanVScores(k1).toListL
        add2 <- sortedSet.zAddIncr(k1, ZArgs.NX, 3, vA)
        elems2 <- sortedSet.zScanVScores(k1).toListL

      } yield {
        //then
        add1 shouldBe 1L
        elems1 should contain theSameElementsAs List(VScore(1, vA))
        add2 shouldBe 0L
        elems2 should contain theSameElementsAs List(VScore(1, vA))
      }
    }.runSyncUnsafe()
  }

  it should "only increment the score if the score has changed" in { //this is not true, it is always incrementing
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAddIncr(k1, ZArgs.CH, 1, vA)
        elems1 <- sortedSet.zScanVScores(k1).toListL
        add2 <- sortedSet.zAddIncr(k1, ZArgs.CH, 1, vA)
        elems2 <- sortedSet.zScanVScores(k1).toListL
        add3 <- sortedSet.zAddIncr(k1, ZArgs.CH, 2, vA)
        elems3 <- sortedSet.zScanVScores(k1).toListL
      } yield {
        //then
        add1 shouldBe 1L
        elems1 should contain theSameElementsAs List(VScore(1, vA))
        add2 shouldBe 2L
        elems2 should contain theSameElementsAs List(VScore(2, vA))
        add3 shouldBe 4L
        elems3 should contain theSameElementsAs List(VScore(4, vA))
      }
    }.runSyncUnsafe()
  }

  it should "only increment the score in existing members, never add new ones" in { //this is not true, it is always incrementing
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        add1 <- sortedSet.zAddIncr(k1, ZArgs.XX, 1, vA)
        elems1 <- sortedSet.zScanVScores(k1).toListL
        _ <- sortedSet.zAdd(k1, 1, vA)
        add2 <- sortedSet.zAddIncr(k1, ZArgs.XX, 1, vA)
        elems2 <- sortedSet.zScanVScores(k1).toListL
        add3 <- sortedSet.zAddIncr(k1, ZArgs.XX, 2, vA)
        elems3 <- sortedSet.zScanVScores(k1).toListL
      } yield {
        //then
        add1 shouldBe 0L
        elems1 shouldBe List.empty
        add2 shouldBe 2L
        elems2 should contain theSameElementsAs List(VScore(2, vA))
        add3 shouldBe 4L
        elems3 should contain theSameElementsAs List(VScore(4, vA))
      }
    }.runSyncUnsafe()
  }

  "zCard" should "return the cardinality of the members" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScores = Gen.listOfN(4, genVScore).sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScores)
        cardinality <- sortedSet.zCard(k1)
      } yield {
        //then
        cardinality shouldBe vScores.size
      }
    }.runSyncUnsafe()
  }

  //todo fix
  "zIncrBy" should "increment the score by the given number" in {
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        incr1 <- sortedSet.zIncrBy(k1, 1.1, vA)
        incr2 <- sortedSet.zIncrBy(k1, 3, vA)
        elems2 <- sortedSet.zScanVScores(k1).toListL
      } yield {
        //then
        incr1 shouldBe 1.1
        incr2 shouldBe 4.1
        elems2 should contain theSameElementsAs List(VScore(4.1, vA))
      }
    }.runSyncUnsafe()
  }

  "zInterStore" should "intersect values between n keys" in {
    //given
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val k3 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get
    val vC = genRedisValue.sample.get
    val vScoreA1 = VScore(1, vA)
    val vScoreA2 = VScore(2, vA)
    val vScoreA3 = VScore(3, vA)
    val vScoreB1 = VScore(1, vB)
    val vScoreB2 = VScore(2, vB)
    val vScoreC1 = VScore(1, vC)

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        intersec0 <- sortedSet.zAdd(k1, vScoreA1) >> sortedSet.zInterStore(k1, k2)
        intersec1 <- sortedSet.zAdd(k2, vScoreA2) >> sortedSet.zInterStore(k1, k2)
        intersec2 <- sortedSet.zAdd(k2, vScoreA1) >> sortedSet.zInterStore(k1, k2)
        intersec3 <- sortedSet.zInterStore(k1, k2, k3)
        _ <- sortedSet.zAdd(k1, vScoreB2, vScoreC1) >> sortedSet.zAdd(k2, vScoreB1, vScoreC1) >> sortedSet.zAdd(k3, vScoreA3, vScoreB2, vScoreC1)
        intersec4 <- sortedSet.zInterStore(k1, k2, k3)
      } yield {
        //then
        0L shouldBe intersec0
        1L shouldBe intersec1
        1L shouldBe intersec2
        0L shouldBe intersec3
        3L shouldBe intersec4
      }
    }.runSyncUnsafe()
  }

  "zLexCount" should "increment the score by the given number" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(1, "A")
    val vScoreB = VScore(1, "B")
    val vScoreC = VScore(2, "C")
    val vScoreD = VScore(1, "D")
    val vScore1 = VScore(1, "1")
    val vScore2 = VScore(1, "2")
    val vScore3 = VScore(1, "3")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeAd <- sortedSet.zLexCount(k1, ZRange("A", "D"))
        range24 <- sortedSet.zLexCount(k1, ZRange("2", "4"))
        rangeGte2 <- sortedSet.zLexCount(k1, ZRange.gte("2")) //counts alpha. members too
        rangeLtA <- sortedSet.zLexCount(k1, ZRange.lt("B")) //includes numbers too
      } yield {
        //then
        4L shouldBe rangeAd //A, B, C D
        2L shouldBe range24 //2, 4
        6L shouldBe rangeGte2 // 2, 3, A, B, C, D
        4L shouldBe rangeLtA //A, 3, 2, 1
      }
    }.runSyncUnsafe()
  }

  "zPopMin" should "remove and return the n lowest scores in the sorted set" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScores = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(n => VScore(n , n.toString))

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScores)
        min1 <- sortedSet.zPopMin(k1)
        min2 <- sortedSet.zPopMin(k1)
        minGroup <- sortedSet.zPopMin(k1, 4).toListL
        count <- sortedSet.zCard(k1)
        emptyVScore <- sortedSet.zPopMin("non-existing-key")
        emptyVScores <- sortedSet.zPopMin("non-existing-key", 10).toListL

      } yield {
        //then
        min1 shouldBe vScores.head
        min2 shouldBe VScore(2, "2")
        minGroup shouldBe List(3, 4, 5, 6).map(n => VScore(n, n.toString))
        count shouldBe 4L
        emptyVScore shouldBe VScore.empty
        emptyVScores shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  "zPopMax" should "remove and return the n lowest scores in the sorted set" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScores = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(n => VScore(n , n.toString))

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScores)
        max1 <- sortedSet.zPopMax(k1)
        max2 <- sortedSet.zPopMax(k1)
        maxGroup <- sortedSet.zPopMax(k1, 4).toListL
        count <- sortedSet.zCard(k1)
        emptyVScore <- sortedSet.zPopMax("non-existing-key")
        emptyVScores <- sortedSet.zPopMax("non-existing-key", 10).toListL

      } yield {
        //then
        max1 shouldBe vScores.last
        max2 shouldBe VScore(9, "9")
        maxGroup shouldBe List(8, 7, 6, 5).map(n => VScore(n, n.toString))
        count shouldBe 4L
        emptyVScore shouldBe VScore.empty
        emptyVScores shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  /*
  "zRange" should "return all the elements in the specified range" in {
    //given
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val vScoreA = VScore(2, "A")
    val vScoreB = VScore(3, "B")
    val vScoreC = VScore(5, "C")
    val vScoreD = VScore(4, "D")
    val vScore1 = VScore(6, "1")
    val vScore2 = VScore(3, "2")
    val vScore3 = VScore(1, "3")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeAll <- sortedSet.zRange(k1, 0, 6).toListL
        rangeEmpty <- sortedSet.zRange(k1, 8, 13).toListL
        range25 <- sortedSet.zRange(k1, 1, 5).toListL
        range34 <- sortedSet.zRange(k1, 2, 4).toListL

      } yield {
        //then
        rangeAll should contain theSameElementsAs List("A", "B", "C", "D", "1", "2", "3")
        rangeEmpty should contain theSameElementsAs List.empty
        range25 should contain theSameElementsAs List("A", "B", "C", "D", "2")
        range34 should contain theSameElementsAs List("B", "D", "2")
      }
    }.runSyncUnsafe()
  }

  "zRangeWithScores" should "return all the elements in the specified range with scores" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(2, "A")
    val vScoreB = VScore(3, "B")
    val vScoreC = VScore(5, "C")
    val vScoreD = VScore(4, "D")
    val vScore1 = VScore(6, "1")
    val vScore2 = VScore(3, "2")
    val vScore3 = VScore(1, "3")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeAll <- sortedSet.zRangeWithScores(k1, 0, 6).toListL
        rangeEmpty <- sortedSet.zRangeWithScores(k1, 8, 13).toListL
        range25 <- sortedSet.zRangeWithScores(k1, 1, 5).toListL
        range34 <- sortedSet.zRangeWithScores(k1, 2, 4).toListL

      } yield {
        //then
        rangeAll should contain theSameElementsAs List(vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeEmpty should contain theSameElementsAs List.empty
        range25 should contain theSameElementsAs List(vScoreA, vScoreB, vScoreC, vScoreD, vScore2)
        range34 should contain theSameElementsAs List(vScoreB, vScoreD, vScore2)
      }
    }.runSyncUnsafe()
  }*/

  "zRangeByLex" should "return all the elements in the specified range with scores" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(1, "A")
    val vScoreB = VScore(1, "B")
    val vScoreC = VScore(2, "C") //even if it is ranged by lex, 'd' goes before 'c' because of the score.
    val vScoreD = VScore(1, "D")
    val vScore1 = VScore(1, "1")
    val vScore2 = VScore(1, "2")
    val vScore3 = VScore(1, "3")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeAd <- sortedSet.zRangeByLex(k1, ZRange("A", "D")).toListL
        range24 <- sortedSet.zRangeByLex(k1, ZRange("2", "4")).toListL
        rangeGte2 <- sortedSet.zRangeByLex(k1, ZRange.gte("2")).toListL //counts alpha. members too
        rangeLtA <- sortedSet.zRangeByLex(k1, ZRange.lt("B")).toListL //includes numbers too
        limit <- sortedSet.zRangeByLex(k1, ZRange.unbounded(), 3, 0).toListL
        limitAndOffset <- sortedSet.zRangeByLex(k1, ZRange.unbounded(), 3, 3).toListL
      } yield {
        //then
        rangeAd should contain theSameElementsAs List("A", "B", "D", "C")
        range24 should contain theSameElementsAs List("2", "3")
        rangeGte2 should contain theSameElementsAs List("A", "B", "C", "D", "2", "3")
        rangeLtA should contain theSameElementsAs List("A", "3", "2", "1")
        // zRange with limit
        limit should contain theSameElementsAs List("1", "2", "3")
        limitAndOffset should contain theSameElementsAs List("A", "B", "D") //d goes first than c because the order is by score first and the n lex
      }
    }.runSyncUnsafe()
  }

  "zRangeByScore" should "return a range of members in a sorted set, by score." in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(2, "A")
    val vScoreB = VScore(3, "B")
    val vScoreC = VScore(5, "C")
    val vScoreD = VScore(4, "D")
    val vScore1 = VScore(6, "1")
    val vScore2 = VScore(3, "2")
    val vScore3 = VScore(1, "3")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeAll <- sortedSet.zRangeByScore(k1, ZRange.unbounded()).toListL
        rangeEmpty <- sortedSet.zRangeByScore(k1, ZRange(8, 13)).toListL
        range25 <- sortedSet.zRangeByScore(k1, ZRange(1, 5)).toListL
        range34 <- sortedSet.zRangeByScore(k1, ZRange(2, 4)).toListL
        lt4 <- sortedSet.zRangeByScore(k1, ZRange.lt(4)).toListL
        gte4 <- sortedSet.zRangeByScore(k1, ZRange.gte(4)).toListL
        limit <- sortedSet.zRangeByScore(k1, ZRange.unbounded(), 4, 0).toListL
        limitAndOffset <- sortedSet.zRangeByScore(k1, ZRange.unbounded(), 4, 4).toListL
      } yield {
        //then
        rangeAll should contain theSameElementsAs List("A", "B", "C", "D", "1", "2", "3")
        rangeEmpty should contain theSameElementsAs List.empty
        range25 should contain theSameElementsAs List("A", "B", "C", "D", "2", "3")
        range34 should contain theSameElementsAs List("A", "B", "D", "2")
        lt4 should contain theSameElementsAs List("3", "2", "A", "B")
        gte4 should contain theSameElementsAs List("D", "C", "1")
        limit should contain theSameElementsAs List("3", "A", "2", "B")
        limitAndOffset should contain theSameElementsAs List("1", "C", "D")
      }
    }.runSyncUnsafe()
  }

  "zRangeByScoreWithScores" should "return a range of members with score in a sorted set, by score." in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(2, "A")
    val vScoreB = VScore(3, "B")
    val vScoreC = VScore(5, "C")
    val vScoreD = VScore(4, "D")
    val vScore1 = VScore(6, "1")
    val vScore2 = VScore(3, "2")
    val vScore3 = VScore(1, "3")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeAll <- sortedSet.zRangeByScoreWithScores(k1, ZRange.unbounded()).toListL
        rangeEmpty <- sortedSet.zRangeByScoreWithScores(k1, ZRange(8, 13)).toListL
        range25 <- sortedSet.zRangeByScoreWithScores(k1, ZRange(1, 5)).toListL
        range34 <- sortedSet.zRangeByScoreWithScores(k1, ZRange(2, 4)).toListL
        lt4 <- sortedSet.zRangeByScoreWithScores(k1, ZRange.lt(4)).toListL
        gte4 <- sortedSet.zRangeByScoreWithScores(k1, ZRange.gte(4)).toListL
        limit <- sortedSet.zRangeByScoreWithScores(k1, ZRange.unbounded(), 4, 0).toListL
        limitAndOffset <- sortedSet.zRangeByScoreWithScores(k1, ZRange.unbounded(), 4, 4).toListL

      } yield {
        //then
        rangeAll should contain theSameElementsAs List(vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeEmpty should contain theSameElementsAs List.empty
        range25 should contain theSameElementsAs List(vScoreA, vScoreB, vScoreC, vScoreD, vScore2, vScore3)
        range34 should contain theSameElementsAs List(vScoreA, vScoreB, vScoreD, vScore2)
        lt4 should contain theSameElementsAs List(vScore3, vScore2, vScoreA, vScoreB)
        gte4 should contain theSameElementsAs List(vScoreD, vScoreC, vScore1)
        limit should contain theSameElementsAs List(vScore3, vScoreA, vScore2, vScoreB)
        limitAndOffset should contain theSameElementsAs List(vScore1, vScoreC, vScoreD)
      }
    }.runSyncUnsafe()
  }

  "zRank" should "the index of a member in a sorted set" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(1, "A")
    val vScoreB = VScore(2, "B")
    val vScore1 = VScore(3, "1")
    //since it is a number will be ranked first then `B` although having the same score
    val vScore2 = VScore(2, "2")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScore1, vScore2)
        rA <- sortedSet.zRank(k1, "A")
        rB <- sortedSet.zRank(k1, "B")
        r1 <- sortedSet.zRank(k1, "1")
        r2 <- sortedSet.zRank(k1, "2")
        rEmpty <- sortedSet.zRank(k1, "5")
      } yield {
        //then
        rA shouldBe Some(0L)
        rB shouldBe Some(2L)
        r1 shouldBe Some(3L)
        r2 shouldBe Some(1L)
        rEmpty shouldBe Option.empty
      }
    }.runSyncUnsafe()
  }

  "zRem" should "remove" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(1, "A")
    val vScoreB = VScore(3, "B")
    val vScore1 = VScore(6, "1")
    val vScore2 = VScore(3, "2")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        count1 <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScore1, vScore2) >> sortedSet.zCard(k1)
        removed1 <- sortedSet.zRem(k1, "A", "1")
        count2 <- sortedSet.zCard(k1)
        removed2 <- sortedSet.zRem(k1, "B", "2", "3")
        count3 <- sortedSet.zCard(k1)
      } yield {
        //then
        count1 shouldBe 4L
        removed1 shouldBe 2L
        count2 shouldBe 2L
        removed2 shouldBe 2L
        count3 shouldBe 0L
      }
    }.runSyncUnsafe()
  }

 "zRemRangeByLex" should "remove all members in a sorted set between the given lexicographical range" in {
    //https://redis.io/commands/zremrangebylex
    //given
    val k1 = genRedisKey.sample.get
    val vScores = List("1", "2", "3", "a", "b", "c", "g").map(VScore(0, _))

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScores)
        removed <- sortedSet.zRemRangeByLex(k1, ZRange("3", "f")) //removes "3" and "f"
        members <- sortedSet.zScanMembers(k1).toListL
      } yield {
        //then
        removed shouldBe 4L
        members shouldBe List("1", "2", "g")
      }
    }.runSyncUnsafe()
  }

  "zRemRangeByScore" should "remove all members in a sorted set within the given scores" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(1, "A")
    val vScoreB = VScore(3, "B")
    val vScoreC = VScore(2, "C")
    val vScore1 = VScore(5, "1")
    val vScore2 = VScore(0, "2")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        count1 <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScore1, vScore2) >> sortedSet.zCard(k1)
        removed1 <- sortedSet.zRemRangeByScore(k1, ZRange(0, 2))
        remaining1 <- sortedSet.zScanMembers(k1).toListL
        removed2 <- sortedSet.zRemRangeByScore(k1, ZRange.gte(1))
      } yield {
        //then
        count1 shouldBe 5L
        removed1 shouldBe 3L
        remaining1 shouldBe List("B", "1")
        removed2 shouldBe 2L
      }
    }.runSyncUnsafe()
  }

  "zRevRangeByLex" should "return a range of members in a sorted set, by lexicographical range ordered from high to low" in {
    //https://redis.io/commands/zRevRangeByLex
    //given
    val k1 = genRedisKey.sample.get
    val vScores = List("a", "b", "c", "d", "e", "f", "g").map(VScore(0, _))

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScores)
        rev1 <- sortedSet.zRevRangeByLex(k1, ZRange.lte("c")).toListL
        rev2 <- sortedSet.zRevRangeByLex(k1, ZRange("aaa", "f")).toListL
        revLimit <- sortedSet.zRevRangeByLex(k1, ZRange.unbounded(), 3).toListL
        revLimitAndOffset <- sortedSet.zRevRangeByLex(k1, ZRange.unbounded(), 3, 2).toListL
      } yield {
        //then
        rev1 shouldBe List("c", "b", "a")
        rev2 shouldBe List("f", "e", "d", "c", "b")
        revLimit shouldBe List("g", "f", "e")
        revLimitAndOffset shouldBe List("e", "d", "c")
      }
    }.runSyncUnsafe()
  }

  "zRevRangeByScore" should "return a range of members in a sorted set, by score, with scores ordered from high to low." in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(1, "A")
    val vScoreB = VScore(3, "B")
    val vScoreC = VScore(2, "C")
    val vScore1 = VScore(4, "1")
    val vScore2 = VScore(0, "2")
    val vScore3 = VScore(2, "3")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScore1, vScore2, vScore3)
        l0 <- sortedSet.zRevRangeByScore(k1, ZRange.unbounded()).toListL
        l1 <- sortedSet.zRevRangeByScore(k1, ZRange(1, 2)).toListL
        l2 <- sortedSet.zRevRangeByScore(k1, ZRange.gt(2)).toListL
        l3 <- sortedSet.zRevRangeByScore(k1, ZRange.lte(2)).toListL
        lLimit <- sortedSet.zRevRangeByScore(k1, ZRange.unbounded(), 4).toListL
        lLimitAndOffset <- sortedSet.zRevRangeByScore(k1, ZRange.unbounded(), 4, 3).toListL

      } yield {
        //then
        l0 shouldBe List("1", "B", "C", "3", "A", "2")
        l1 shouldBe List("C", "3", "A")
        l2 shouldBe List("1", "B")
        l3 shouldBe List("C", "3", "A", "2")
        lLimit shouldBe List("1", "B", "C", "3")
        lLimitAndOffset shouldBe List("3", "A", "2")
      }
    }.runSyncUnsafe()
  }

  "zRevRangeByScoreWithScores" should "return a range of members with scores in a sorted set, by score, with scores ordered from high to low" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore(1, "A")
    val vScoreB = VScore(3, "B")
    val vScoreC = VScore(2, "C")
    val vScore1 = VScore(4, "1")
    val vScore2 = VScore(0, "2")
    val vScore3 = VScore(2, "3")

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScore1, vScore2, vScore3)
        l0 <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.unbounded()).toListL
        l1 <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange(1, 2)).toListL
        l2 <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.gt(2)).toListL
        l3 <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.lte(2)).toListL
        lLimit <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.unbounded(), 4).toListL
        lLimitAndOffset <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.unbounded(), 4, 3).toListL
      } yield {
        //then
        l0 shouldBe List(vScore1, vScoreB, vScoreC, vScore3, vScoreA, vScore2)
        l1 shouldBe List(vScoreC, vScore3, vScoreA)
        l2 shouldBe List(vScore1, vScoreB)
        l3 shouldBe List(vScoreC, vScore3, vScoreA, vScore2)
        lLimit shouldBe List(vScore1, vScoreB, vScoreC, vScore3)
        lLimitAndOffset shouldBe List(vScore3, vScoreA, vScore2)
      }
    }.runSyncUnsafe()
  }

  "zScan" should "return all members with scores" in {
    //given
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val vScores = Gen.listOfN(15, genVScore).sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScores)
        scanMembers <- sortedSet.zScanMembers(k1).toListL
        emptyMembers <- sortedSet.zScanMembers(k2).toListL
        scanVScores <- sortedSet.zScanVScores(k1).toListL
        emptyVcores <- sortedSet.zScanVScores(k2).toListL
      } yield {
        //then
        scanMembers should contain theSameElementsAs vScores.map(_.value.get)
        emptyMembers shouldBe List.empty
        scanVScores should contain theSameElementsAs vScores
        emptyVcores shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  "zScore" should "ret" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScores = Gen.listOfN(15, genVScore).sample.get
    val nonExistingMember = Gen.identifier.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScores)
        scores <- Task.traverse(vScores)(vScore => sortedSet.zScore(k1, vScore.value.get))
        emptyScore <- sortedSet.zScore(k1, nonExistingMember)
      } yield {
        //then
        scores should contain theSameElementsAs vScores.map(_.score)
        emptyScore shouldBe 0.0
      }
    }.runSyncUnsafe()
  }

  "zUnionStore" should "add multiple sorted sets and store the resulting sorted set in a new key" in {
    //given
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val k3 = genRedisKey.sample.get
    val k4 = genRedisKey.sample.get

    val vScores1 = List("A", "B", "C", "D").map(VScore(1, _))
    val vScores2 = List("1", "2", "3").map(VScore(1, _))
    val vScoresDuplicated = List("1", "2").map(VScore(3, _))

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        //when
        _ <- sortedSet.zAdd(k1, vScores1) >> sortedSet.zAdd(k2, vScores2)  >> sortedSet.zAdd(k3, vScoresDuplicated)
        union <- sortedSet.zUnionStore(k4, k1, k2, k3)
        scan <- sortedSet.zScanMembers(k4).toListL
      } yield {
        //then
        union shouldBe vScores1.size + vScores2.size
        scan should contain theSameElementsAs vScores1.map(_.value.get) ++ vScores2.map(_.value.get)
      }
    }.runSyncUnsafe()
  }

}
