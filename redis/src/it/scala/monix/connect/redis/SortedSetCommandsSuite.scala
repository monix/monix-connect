package monix.connect.redis

import monix.connect.redis.client.RedisCmd
import monix.connect.redis.domain.{VScore, ZArgs, ZRange}
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class SortedSetCommandsSuite
  extends AsyncFlatSpec with MonixTaskSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  override implicit val scheduler: Scheduler = Scheduler.io("sorted-set-commands-suite")

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll).runSyncUnsafe()
  }

  s"zAdd" should "insert elements with no order and reading back sorted" in {
    val k: K = genRedisKey.sample.get
    val v0: String = Gen.alphaLowerStr.sample.get
    val v1: String = Gen.alphaLowerStr.sample.get
    val v2: String = Gen.alphaLowerStr.sample.get
    val minScore: Double = 1
    val middleScore: Double = 3
    val maxScore: Double = 4
    val incrby: Double = 2

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k, minScore, v0)
        _ <- sortedSet.zAdd(k, middleScore, v1)
        _ <- sortedSet.zAdd(k, maxScore, v2)
        _ <- sortedSet.zIncrBy(k, incrby, v1)
        min <- sortedSet.zPopMin(k)
        max <- sortedSet.zPopMax(k)
      } yield {
        min.score shouldBe minScore
        min.value shouldBe Some(v0)
        max.score shouldBe middleScore + incrby
        max.value shouldBe Some(v1)
      }
    }
  }

  //todo revise
  "bZPopMin" should "remove and return the member with lowest score from one of the keys" in {
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val vScore1 = VScore("a", 1)
    val vScore2 = VScore("b", 2)
    val vScore3 = VScore("c", 3)
    val vScore4 = VScore("d", 4)
    val vScore5 = VScore("e", 5)


    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScore1, vScore2, vScore4, vScore5)
        _ <- sortedSet.zAdd(k2, vScore3)
        min1 <- sortedSet.bZPopMin(1.seconds, k1, k2)
        min2 <- sortedSet.bZPopMin(1.seconds, k1, k2)
        min4 <- sortedSet.bZPopMin(1.seconds, k1, k2)
        min5 <- sortedSet.bZPopMin(1.seconds, k1, k2)
        min3 <- sortedSet.bZPopMin(1.seconds, k1, k2)
        empty <- sortedSet.bZPopMin(1.seconds, k1, k2)
      } yield {
        min1 shouldBe Some(k1, vScore1)
        min2 shouldBe Some(k1, vScore2) // returns 5 because is the min score in k1
        min4 shouldBe Some(k1, vScore4)
        min5 shouldBe Some(k1, vScore5)
        min3 shouldBe Some(k2, vScore3)
        empty shouldBe None
      }
    }

  }

  //todo revise
  "bZPopMax" should "remove and return the member with lowest score from one of the keys" in {
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val vScore1 = genVScore(1).sample.get
    val vScore3 = genVScore(3).sample.get
    val vScore5 = genVScore(5).sample.get

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScore1, vScore5)
        _ <- sortedSet.zAdd(k2, vScore3)
        firstMax <- sortedSet.bZPopMax(1.seconds, k1, k2)
        secondMax <- sortedSet.bZPopMax(1.seconds, k1, k2)
        thirdMax <- sortedSet.bZPopMax(1.seconds, k1, k2)
        empty <- sortedSet.bZPopMax(1.seconds, k1, k2)
      } yield {
        firstMax shouldBe Some(k1, vScore5)
        secondMax shouldBe Some(k1, vScore1) // returns 5 because is the min score in k1
        thirdMax shouldBe Some(k2, vScore3)
        empty shouldBe None
      }
    }
  }

  "zAdd" should "add a single scored values to the sorted set" in {
    val k1 = genRedisKey.sample.get
    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAdd(k1, 1, k1)
        add2 <- sortedSet.zAdd(k1, 5, k1)
        size1 <- sortedSet.zCard(k1)
        value <- sortedSet.zPopMin(k1)
      } yield {
        add1 shouldBe true
        add2 shouldBe false //false if the value already existed
        size1 shouldBe 1L
        value.score shouldBe 5
      }
    }
  }

  it should "add multiple scored values to the sorted set and count only the new inserted ones, not the updates" in {
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val vScore1 = genVScore(1).sample.get
    val vScore2 = genVScore(2).sample.get
    val vScore3 = genVScore(3).sample.get
    utfConnection.use[Task, Assertion]  { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAdd(k1, vScore1)
        add2 <- sortedSet.zAdd(k2, vScore2, vScore3)
        add3 <- sortedSet.zAdd(k2, List(vScore1, vScore2, vScore3))
        size1 <- sortedSet.zCard(k1)
        size2 <- sortedSet.zCard(k2)
      } yield {
        add1 shouldBe 1L
        add2 shouldBe 2L
        add3 shouldBe 1L
        size1 shouldBe 1L
        size2 shouldBe 3L
      }
    }
  }

  "zAddNx" should "adds one member to the set and never update the score if it already existed" in {
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        addFirst <- sortedSet.zAdd(k1, ZArgs.NX, 1, vA)
        addSameScore <- sortedSet.zAdd(k1, ZArgs.NX, 1, vA)
        elems1 <- sortedSet.zRevGetAll(k1).toListL
        addDifScore <- sortedSet.zAdd(k1, ZArgs.NX, 2, vA)
        elems2 <- sortedSet.zRevGetAll(k1).toListL
        addDifVal <- sortedSet.zAdd(k1, ZArgs.NX, 1, vB)
        elems3 <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        addFirst shouldBe 1L
        addSameScore shouldBe 0L
        elems1 shouldBe List(VScore(vA, 1))
        addDifScore shouldBe 0L
        elems2 shouldBe List(VScore(vA, 1))
        addDifVal shouldBe 1L
        elems3 should contain theSameElementsAs List(VScore(vA, 1), VScore(vB, 1))
      }
    }
  }

  it should "add multiple members to the set but never update the score if it already existed" in {
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get
    val vC = genRedisValue.sample.get
    val vScoreA1 = VScore(vA, 1)
    val vScoreA2 = VScore(vA, 2)
    val vScoreB1 = VScore(vB, 3)
    val vScoreC1 = VScore(vC, 4)

    utfConnection.use[Task, Assertion]{ case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAdd(k1, ZArgs.NX, vScoreA1)
        add2 <- sortedSet.zAdd(k1, ZArgs.NX, List(vScoreA2, vScoreB1, vScoreC1))
        elems <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        add1 shouldBe 1L
        add2 shouldBe 2L
        elems should contain theSameElementsAs List(vScoreA1, vScoreB1, vScoreC1)
      }
    }
  }

  "zAddCh" should "add multiple members to the set and count all that changed" in {
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get
    val vC = genRedisValue.sample.get
    val vScoreA2 = VScore(vA, 2)
    val vScoreB1 = VScore(vB, 3)
    val vScoreC1 = VScore(vC, 4)

    utfConnection.use[Task, Assertion]  { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAdd(k1, ZArgs.CH, 1, vA)
        add2 <- sortedSet.zAdd(k1, ZArgs.CH, List(vScoreA2, vScoreB1, vScoreC1))
        add3 <- sortedSet.zAdd(k1, ZArgs.CH, 2, vA)
        elems <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        add1 shouldBe 1L
        add2 shouldBe 3L
        add3 shouldBe 0L
        elems should contain theSameElementsAs List(vScoreA2, vScoreB1, vScoreC1)
      }
    }
  }

  "zAddXx" should "only update members, never add new ones" in {
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get
    val vC = genRedisValue.sample.get
    val vScoreA1 = VScore(vA, 1)
    val vScoreA2 = VScore(vA, 2)
    val vScoreA3 = VScore(vA, 3)
    val vScoreB1 = VScore(vB, 1)
    val vScoreB2 = VScore(vB, 2)
    val vScoreC1 = VScore(vC, 1)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAdd(k1, ZArgs.XX, 1, vA)
        elems1 <- sortedSet.zRevGetAll(k1).toListL
        _ <- sortedSet.zAdd(k1, vScoreA1, vScoreB1)
        add2 <- sortedSet.zAdd(k1, ZArgs.XX, vScoreA2)
        elems2 <- sortedSet.zRevGetAll(k1).toListL
        add3 <- sortedSet.zAdd(k1, ZArgs.XX, vScoreA3, vScoreB2, vScoreC1)
        elems3 <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        add1 shouldBe 0L
        elems1 shouldBe List.empty
        add2 shouldBe 0L
        elems2 should contain theSameElementsAs List(vScoreA2, vScoreB1)
        add3 shouldBe 0L
        elems3 should contain theSameElementsAs List(vScoreA3, vScoreB2)
      }
    }
  }

  "zAddIncr" should "increments the score by the given number" in {
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use[Task, Assertion]  { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAddIncr(k1, 1, vA)
        elems1 <- sortedSet.zRevGetAll(k1).toListL
        add2 <- sortedSet.zAddIncr(k1, 3, vA)
        elems2 <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        add1 shouldBe 1L
        elems1 should contain theSameElementsAs List(VScore(vA, 1))
        add2 shouldBe 4L
        elems2 should contain theSameElementsAs List(VScore(vA, 4))
      }
    }
  }

  "zAddIncrNx" should "not increment the score if it already exists" in {
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAddIncr(k1, ZArgs.NX, 1, vA)
        elems1 <- sortedSet.zRevGetAll(k1).toListL
        add2 <- sortedSet.zAddIncr(k1, ZArgs.NX, 3, vA)
        elems2 <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        add1 shouldBe 1L
        elems1 should contain theSameElementsAs List(VScore(vA, 1))
        add2 shouldBe 0L
        elems2 should contain theSameElementsAs List(VScore(vA, 1))
      }
    }
  }

  //todo this is not true, it is always incrementing
  "zAddIncrCh" should "only increment the score if the score has changed" in {
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAddIncr(k1, ZArgs.CH, 1, vA)
        elems1 <- sortedSet.zRevGetAll(k1).toListL
        add2 <- sortedSet.zAddIncr(k1, ZArgs.CH, 1, vA)
        elems2 <- sortedSet.zRevGetAll(k1).toListL
        add3 <- sortedSet.zAddIncr(k1, ZArgs.CH, 2, vA)
        elems3 <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        add1 shouldBe 1L
        elems1 should contain theSameElementsAs List(VScore(vA, 1))
        add2 shouldBe 2L
        elems2 should contain theSameElementsAs List(VScore(vA, 2))
        add3 shouldBe 4L
        elems3 should contain theSameElementsAs List(VScore(vA, 4))
      }
    }
  }

  it should "only increment the score in existing members, never add new ones" in { //this is not true, it is always incrementing
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        add1 <- sortedSet.zAddIncr(k1, ZArgs.XX, 1, vA)
        elems1 <- sortedSet.zRevGetAll(k1).toListL
        _ <- sortedSet.zAdd(k1, 1, vA)
        add2 <- sortedSet.zAddIncr(k1, ZArgs.XX, 1, vA)
        elems2 <- sortedSet.zRevGetAll(k1).toListL
        add3 <- sortedSet.zAddIncr(k1, ZArgs.XX, 2, vA)
        elems3 <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        add1 shouldBe 0L
        elems1 shouldBe List.empty
        add2 shouldBe 2L
        elems2 should contain theSameElementsAs List(VScore(vA, 2))
        add3 shouldBe 4L
        elems3 should contain theSameElementsAs List(VScore(vA, 4))
      }
    }
  }

  "zCard" should "return the cardinality of the members" in {
    val k1 = genRedisKey.sample.get
    val vScores = Gen.listOfN(4, genVScore).sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      sortedSet.zAdd(k1, vScores) >>
        sortedSet.zCard(k1)
    }.asserting(_ shouldBe vScores.size)
  }

  "zCount" should "count the number of members in a sorted set between a given ZRange." in {
    val k1 = genRedisKey.sample.get
    val vScore1 = VScore("1", 1)
    val vScore2 = VScore("2", 2)
    val vScore3 = VScore("3", 3)
    val vScoreA = VScore("A", 4)
    val vScoreB = VScore("B", 5)
    val vScoreC = VScore("C", 6)
    val vScoreD = VScore("D", 7)

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      sortedSet.zAdd(k1, vScore1, vScore2, vScore3, vScoreA, vScoreB, vScoreC, vScoreD) *>
        sortedSet.zCount(k1, ZRange(2, 5))
    }.asserting(_ shouldBe 4)
  }

  "zIncrBy" should "increment the score by the given number" in {
    //given
    val k1 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        incr1 <- sortedSet.zIncrBy(k1, 1.1, vA)
        incr2 <- sortedSet.zIncrBy(k1, 3, vA)
        elems2 <- sortedSet.zRevGetAll(k1).toListL
      } yield {
        incr1 shouldBe 1.1
        incr2 shouldBe 4.1
        elems2 should contain theSameElementsAs List(VScore(vA, 4.1))
      }
    }
  }

  "zInterStore" should "intersect values between n keys" in {
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val k3 = genRedisKey.sample.get
    val vA = genRedisValue.sample.get
    val vB = genRedisValue.sample.get
    val vC = genRedisValue.sample.get
    val vScoreA1 = VScore(vA, 1)
    val vScoreA2 = VScore(vA, 2)
    val vScoreA3 = VScore(vA, 3)
    val vScoreB1 = VScore(vB, 1)
    val vScoreB2 = VScore(vB, 2)
    val vScoreC1 = VScore(vC, 1)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        intersec0 <- sortedSet.zAdd(k1, vScoreA1) >> sortedSet.zInterStore(k1, k2)
        intersec1 <- sortedSet.zAdd(k2, vScoreA2) >> sortedSet.zInterStore(k1, k2)
        intersec2 <- sortedSet.zAdd(k2, vScoreA1) >> sortedSet.zInterStore(k1, k2)
        intersec3 <- sortedSet.zInterStore(k1, k2, k3)
        _ <- sortedSet.zAdd(k1, vScoreB2, vScoreC1) >> sortedSet.zAdd(k2, vScoreB1, vScoreC1) >> sortedSet.zAdd(k3, vScoreA3, vScoreB2, vScoreC1)
        intersec4 <- sortedSet.zInterStore(k1, k2, k3)
      } yield {
        0L shouldBe intersec0
        1L shouldBe intersec1
        1L shouldBe intersec2
        0L shouldBe intersec3
        3L shouldBe intersec4
      }
    }
  }

  "zLexCount" should "increment the score by the given number" in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 1)
    val vScoreB = VScore("B", 1)
    val vScoreC = VScore("C", 2)
    val vScoreD = VScore("D", 1)
    val vScore1 = VScore("1", 1)
    val vScore2 = VScore("2", 1)
    val vScore3 = VScore("3", 1)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeAd <- sortedSet.zLexCount(k1, ZRange("A", "D"))
        range24 <- sortedSet.zLexCount(k1, ZRange("2", "4"))
        rangeGte2 <- sortedSet.zLexCount(k1, ZRange.gte("2")) //counts alpha. members too
        rangeLtA <- sortedSet.zLexCount(k1, ZRange.lt("B")) //includes numbers too
      } yield {
        4L shouldBe rangeAd //A, B, C D
        2L shouldBe range24 //2, 4
        6L shouldBe rangeGte2 // 2, 3, A, B, C, D
        4L shouldBe rangeLtA //A, 3, 2, 1
      }
    }
  }

  "zPopMin" should "remove and return the n lowest scores in the sorted set" in {
    val k1 = genRedisKey.sample.get
    val vScores = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(n => VScore(n.toString, n))

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScores)
        min1 <- sortedSet.zPopMin(k1)
        min2 <- sortedSet.zPopMin(k1)
        minGroup <- sortedSet.zPopMin(k1, 4).toListL
        count <- sortedSet.zCard(k1)
        emptyVScore <- sortedSet.zPopMin("non-existing-key")
        emptyVScores <- sortedSet.zPopMin("non-existing-key", 10).toListL
      } yield {
        min1 shouldBe vScores.head
        min2 shouldBe VScore("2", 2)
        minGroup shouldBe List(3, 4, 5, 6).map(n => VScore(n.toString, n))
        count shouldBe 4L
        emptyVScore shouldBe VScore.empty
        emptyVScores shouldBe List.empty
      }
    }
  }

  "zPopMax" should "remove and return the n lowest scores in the sorted set" in {
    val k1 = genRedisKey.sample.get
    val vScores = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(n => VScore(n.toString, n))

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScores)
        max1 <- sortedSet.zPopMax(k1)
        max2 <- sortedSet.zPopMax(k1)
        maxGroup <- sortedSet.zPopMax(k1, 4).toListL
        count <- sortedSet.zCard(k1)
        emptyVScore <- sortedSet.zPopMax("non-existing-key")
        emptyVScores <- sortedSet.zPopMax("non-existing-key", 10).toListL
      } yield {
        max1 shouldBe vScores.last
        max2 shouldBe VScore("9", 9)
        maxGroup shouldBe List(8, 7, 6, 5).map(n => VScore(n.toString, n))
        count shouldBe 4L
        emptyVScore shouldBe VScore.empty
        emptyVScores shouldBe List.empty
      }
    }
  }

  "zRange" should "return all the elements in the specified range with scores" in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 2)
    val vScoreB = VScore("B", 3)
    val vScoreC = VScore("C", 4)
    val vScoreD = VScore("D", 5)
    val vScore1 = VScore("1", 1)
    val vScore2 = VScore("2", 2)
    val vScore3 = VScore("3", 4)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        range01 <- sortedSet.zRange(k1, 0, 1).toListL
        range24 <- sortedSet.zRange(k1, 2, 4).toListL
        range37 <- sortedSet.zRange(k1, 3, 7).toListL
        outOfRange <- sortedSet.zRange(k1, 10, 11).toListL
      } yield {
        range01 should contain theSameElementsAs List("1", "2")
        range24 should contain theSameElementsAs List("A", "B", "3")
        range37 should contain theSameElementsAs List("B", "3", "C", "D")
        outOfRange shouldBe List.empty
      }
    }
  }

  "zRangeByLex" should "return all the elements in the specified range with scores" in {
    //given
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 1)
    val vScoreB = VScore("B", 1)
    val vScoreC = VScore("C", 2) //even if it is ranged by lex, 'd' goes before 'c' because of the score.
    val vScoreD = VScore("D", 1)
    val vScore1 = VScore("1", 1)
    val vScore2 = VScore("2", 1)
    val vScore3 = VScore("3", 1)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeAd <- sortedSet.zRangeByLex(k1, ZRange("A", "D")).toListL
        range24 <- sortedSet.zRangeByLex(k1, ZRange("2", "4")).toListL
        rangeGte2 <- sortedSet.zRangeByLex(k1, ZRange.gte("2")).toListL //counts alpha. members too
        rangeLtA <- sortedSet.zRangeByLex(k1, ZRange.lt("B")).toListL //includes numbers too
        limit <- sortedSet.zRangeByLex(k1, ZRange.unbounded(), 3, 0).toListL
        limitAndOffset <- sortedSet.zRangeByLex(k1, ZRange.unbounded(), 3, 3).toListL
      } yield {
        rangeAd should contain theSameElementsAs List("A", "B", "D", "C")
        range24 should contain theSameElementsAs List("2", "3")
        rangeGte2 should contain theSameElementsAs List("A", "B", "C", "D", "2", "3")
        rangeLtA should contain theSameElementsAs List("A", "3", "2", "1")
        // zRange with limit
        limit should contain theSameElementsAs List("1", "2", "3")
        limitAndOffset should contain theSameElementsAs List("A", "B", "D") //d goes first than c because the order is by score first and the n lex
      }
    }
  }

  "zRangeByScore" should "return a range of members in a sorted set, by score." in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 2)
    val vScoreB = VScore("B", 3)
    val vScoreC = VScore("C", 5)
    val vScoreD = VScore("D", 4)
    val vScore1 = VScore("1", 6)
    val vScore2 = VScore("2", 3)
    val vScore3 = VScore("3", 1)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
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
        rangeAll should contain theSameElementsAs List("A", "B", "C", "D", "1", "2", "3")
        rangeEmpty should contain theSameElementsAs List.empty
        range25 should contain theSameElementsAs List("A", "B", "C", "D", "2", "3")
        range34 should contain theSameElementsAs List("A", "B", "D", "2")
        lt4 should contain theSameElementsAs List("3", "2", "A", "B")
        gte4 should contain theSameElementsAs List("D", "C", "1")
        limit should contain theSameElementsAs List("3", "A", "2", "B")
        limitAndOffset should contain theSameElementsAs List("1", "C", "D")
      }
    }
  }

  "zRangeByScoreWithScores" should "return a range of members with score in a sorted set, by score." in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 2)
    val vScoreB = VScore("B", 3)
    val vScoreC = VScore("C", 5)
    val vScoreD = VScore("D", 4)
    val vScore1 = VScore("1", 6)
    val vScore2 = VScore("2", 3)
    val vScore3 = VScore("3", 1)
    utfConnection.use[Task, Assertion]  { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
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
        rangeAll should contain theSameElementsAs List(vScoreA, vScoreB, vScoreC, vScoreD, vScore1, vScore2, vScore3)
        rangeEmpty should contain theSameElementsAs List.empty
        range25 should contain theSameElementsAs List(vScoreA, vScoreB, vScoreC, vScoreD, vScore2, vScore3)
        range34 should contain theSameElementsAs List(vScoreA, vScoreB, vScoreD, vScore2)
        lt4 should contain theSameElementsAs List(vScore3, vScore2, vScoreA, vScoreB)
        gte4 should contain theSameElementsAs List(vScoreD, vScoreC, vScore1)
        limit should contain theSameElementsAs List(vScore3, vScoreA, vScore2, vScoreB)
        limitAndOffset should contain theSameElementsAs List(vScore1, vScoreC, vScoreD)
      }
    }
  }

  "zRank" should "the index of a member in a sorted set" in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 1)
    val vScoreB = VScore("B", 2)
    val vScore1 = VScore("1", 3)
    //since it is a number will be ranked first then `B` although having the same score
    val vScore2 = VScore("2", 2)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScore1, vScore2)
        rA <- sortedSet.zRank(k1, "A")
        rB <- sortedSet.zRank(k1, "B")
        r1 <- sortedSet.zRank(k1, "1")
        r2 <- sortedSet.zRank(k1, "2")
        rEmpty <- sortedSet.zRank(k1, "5")
      } yield {
        rA shouldBe Some(0L)
        rB shouldBe Some(2L)
        r1 shouldBe Some(3L)
        r2 shouldBe Some(1L)
        rEmpty shouldBe Option.empty
      }
    }
  }

  "zRem" should "remove" in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 1)
    val vScoreB = VScore("B", 3)
    val vScore1 = VScore("1", 6)
    val vScore2 = VScore("2", 3)

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        count1 <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScore1, vScore2) >> sortedSet.zCard(k1)
        removed1 <- sortedSet.zRem(k1, "A", "1")
        count2 <- sortedSet.zCard(k1)
        removed2 <- sortedSet.zRem(k1, "B", "2", "3")
        count3 <- sortedSet.zCard(k1)
      } yield {
        count1 shouldBe 4L
        removed1 shouldBe 2L
        count2 shouldBe 2L
        removed2 shouldBe 2L
        count3 shouldBe 0L
      }
    }
  }

  "zRemRangeByLex" should "remove all members in a sorted set between the given lexicographical range" in {
    //https://redis.io/commands/zremrangebylex
    val k1 = genRedisKey.sample.get
    val vScores = List("1", "2", "3", "a", "b", "c", "g").map(VScore(_, 0))

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScores)
        removed <- sortedSet.zRemRangeByLex(k1, ZRange("3", "f")) //removes "3" and "f"
        members <- sortedSet.zMembers(k1).toListL
      } yield {
        removed shouldBe 4L
        members shouldBe List("1", "2", "g")
      }
    }
  }

  "zRemRangeByScore" should "remove all members in a sorted set within the given scores" in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 1)
    val vScoreB = VScore("B", 3)
    val vScoreC = VScore("C", 2)
    val vScore1 = VScore("1", 5)
    val vScore2 = VScore("2", 0)

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        count1 <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScore1, vScore2) >> sortedSet.zCard(k1)
        removed1 <- sortedSet.zRemRangeByScore(k1, ZRange(0, 2))
        remaining1 <- sortedSet.zMembers(k1).toListL
        removed2 <- sortedSet.zRemRangeByScore(k1, ZRange.gte(1))
      } yield {
        count1 shouldBe 5L
        removed1 shouldBe 3L
        remaining1 shouldBe List("B", "1")
        removed2 shouldBe 2L
      }
    }
  }

  "zRevRangeByLex" should "return a range of members in a sorted set, by lexicographical range ordered from high to low" in {
    //https://redis.io/commands/zRevRangeByLex
    val k1 = genRedisKey.sample.get
    val vScores = List("a", "b", "c", "d", "e", "f", "g").map(VScore(_, 0))

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScores)
        rev1 <- sortedSet.zRevRangeByLex(k1, ZRange.lte("c")).toListL
        rev2 <- sortedSet.zRevRangeByLex(k1, ZRange("aaa", "f")).toListL
        revLimit <- sortedSet.zRevRangeByLex(k1, ZRange.unbounded(), 3).toListL
        revLimitAndOffset <- sortedSet.zRevRangeByLex(k1, ZRange.unbounded(), 3, 2).toListL
      } yield {
        rev1 shouldBe List("c", "b", "a")
        rev2 shouldBe List("f", "e", "d", "c", "b")
        revLimit shouldBe List("g", "f", "e")
        revLimitAndOffset shouldBe List("e", "d", "c")
      }
    }
  }

  "zRevRangeByScore" should "return a range of members in a sorted set, by score, with scores ordered from high to low." in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 1)
    val vScoreB = VScore("B", 3)
    val vScoreC = VScore("C", 2)
    val vScore1 = VScore("1", 4)
    val vScore2 = VScore("2", 0)
    val vScore3 = VScore("3", 2)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScore1, vScore2, vScore3)
        l0 <- sortedSet.zRevRangeByScore(k1, ZRange.unbounded()).toListL
        l1 <- sortedSet.zRevRangeByScore(k1, ZRange(1, 2)).toListL
        l2 <- sortedSet.zRevRangeByScore(k1, ZRange.gt(2)).toListL
        l3 <- sortedSet.zRevRangeByScore(k1, ZRange.lte(2)).toListL
        lLimit <- sortedSet.zRevRangeByScore(k1, ZRange.unbounded(), 4).toListL
        lLimitAndOffset <- sortedSet.zRevRangeByScore(k1, ZRange.unbounded(), 4, 3).toListL
      } yield {
        l0 shouldBe List("1", "B", "C", "3", "A", "2")
        l1 shouldBe List("C", "3", "A")
        l2 shouldBe List("1", "B")
        l3 shouldBe List("C", "3", "A", "2")
        lLimit shouldBe List("1", "B", "C", "3")
        lLimitAndOffset shouldBe List("3", "A", "2")
      }
    }
  }

  "zRevRangeByScoreWithScores" should "return a range of members with scores in a sorted set, by score, with scores ordered from high to low" in {
    val k1 = genRedisKey.sample.get
    val vScoreA = VScore("A", 1)
    val vScoreB = VScore("B", 3)
    val vScoreC = VScore("C", 2)
    val vScore1 = VScore("1", 4)
    val vScore2 = VScore("2", 0)
    val vScore3 = VScore("3", 2)

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScoreA, vScoreB, vScoreC, vScore1, vScore2, vScore3)
        l0 <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.unbounded()).toListL
        l1 <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange(1, 2)).toListL
        l2 <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.gt(2)).toListL
        l3 <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.lte(2)).toListL
        lLimit <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.unbounded(), 4).toListL
        lLimitAndOffset <- sortedSet.zRevRangeByScoreWithScores(k1, ZRange.unbounded(), 4, 3).toListL
      } yield {
        l0 shouldBe List(vScore1, vScoreB, vScoreC, vScore3, vScoreA, vScore2)
        l1 shouldBe List(vScoreC, vScore3, vScoreA)
        l2 shouldBe List(vScore1, vScoreB)
        l3 shouldBe List(vScoreC, vScore3, vScoreA, vScore2)
        lLimit shouldBe List(vScore1, vScoreB, vScoreC, vScore3)
        lLimitAndOffset shouldBe List(vScore3, vScoreA, vScore2)
      }
    }
  }

  "all members" can "be fetched and ordered from high to low and viceversa" in {
    val k1 = genRedisKey.sample.get
    val vScores = List(
      VScore("A", 1),
      VScore("B", 2),
      VScore("C", 3),
      VScore("D", 4),
      VScore("E", 5),
      VScore("F", 6)
    )

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScores)
        members <- sortedSet.zMembers(k1).toListL
        emptyMembers <- sortedSet.zMembers("non-existing-key").toListL
        revMembers <- sortedSet.zRevMembers(k1).toListL
        revEmptyMembers <- sortedSet.zRevMembers("non-existing-key").toListL

      } yield {
        members shouldBe vScores.map(_.value.get)
        revMembers shouldBe vScores.map(_.value.get).reverse
        emptyMembers shouldBe List.empty
        revEmptyMembers shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  "all value with score" can "be fetched and ordered from low to high and viceversa" in {
    val k1 = genRedisKey.sample.get
    val vScores = List(
      VScore("A", 1),
      VScore("B", 2),
      VScore("C", 3),
      VScore("D", 4),
      VScore("E", 5),
      VScore("F", 6)
    )

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScores)
        getAllVScores <- sortedSet.zGetAll(k1).toListL
        emptyVScores <- sortedSet.zGetAll("non-existing-key").toListL
        revGetAllVScores <- sortedSet.zRevGetAll(k1).toListL
        revEmptyVScores <- sortedSet.zRevGetAll("non-existing-key").toListL
      } yield {
        getAllVScores shouldBe vScores
        revGetAllVScores shouldBe vScores.reverse
        emptyVScores shouldBe List.empty
        revEmptyVScores shouldBe List.empty
      }
    }
  }

  "zScore" should "get the score associated with the given member in a sorted set" in {
    val k1 = genRedisKey.sample.get
    val vScores = Gen.listOfN(15, genVScore).sample.get
    val nonExistingMember = Gen.identifier.sample.get

    utfConnection.use { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScores)
        scores <- Task.traverse(vScores)(vScore => sortedSet.zScore(k1, vScore.value.get))
        emptyScore <- sortedSet.zScore(k1, nonExistingMember)
      } yield {
        scores should contain theSameElementsAs vScores.map(_.score)
        emptyScore shouldBe 0.0
      }
    }
  }

  "zUnionStore" should "add multiple sorted sets and store the resulting sorted set in a new key" in {
    val k1 = genRedisKey.sample.get
    val k2 = genRedisKey.sample.get
    val k3 = genRedisKey.sample.get
    val k4 = genRedisKey.sample.get

    val vScores1 = List("A", "B", "C", "D").map(VScore(_, 1))
    val vScores2 = List("1", "2", "3").map(VScore(_, 1))
    val vScoresDuplicated = List("1", "2").map(VScore(_, 3))

    utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, _, sortedSet, _) =>
      for {
        _ <- sortedSet.zAdd(k1, vScores1) >> sortedSet.zAdd(k2, vScores2) >> sortedSet.zAdd(k3, vScoresDuplicated)
        union <- sortedSet.zUnionStore(k4, k1, k2, k3)
        scan <- sortedSet.zMembers(k4).toListL
      } yield {
        union shouldBe vScores1.size + vScores2.size
        scan should contain theSameElementsAs vScores1.map(_.value.get) ++ vScores2.map(_.value.get)
      }
    }
  }

}
