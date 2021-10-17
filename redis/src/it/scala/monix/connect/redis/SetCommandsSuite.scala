package monix.connect.redis

import monix.connect.redis.client.RedisCmd
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class SetCommandsSuite
  extends AsyncFlatSpec with MonixTaskSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  override implicit val scheduler: Scheduler = Scheduler.io("set-commands-suite")

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll).runSyncUnsafe()
  }

  "sAdd" should "add one or more members to a set" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        one <- cmd.set.sAdd(k1, values.head)
        size <- cmd.set.sAdd(k2, values: _*)
        members <- cmd.set.sMembers(k2).toListL
      } yield {
        one shouldBe 1L
        size shouldBe values.size
        members should contain theSameElementsAs values
      }
    }
  }

  "sCard" should "get the number of members in a set" in {
    val k: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        size <- cmd.set.sAdd(k, values: _*)
        card <- cmd.set.sCard(k)
      } yield {
        card shouldBe values.size
        card shouldBe size
      }
    }
  }

  "sDiff" should "subtract the first set with the successive ones" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val set1: List[V] = List("a", "b", "c", "d")
    val set2: List[V] = List("c", "d", "e")

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        size1 <- cmd.set.sAdd(k1, set1)
        size2 <- cmd.set.sAdd(k2, set2)
        diff <- cmd.set.sDiff(k1, k2).toListL
        noDiff <- cmd.set.sDiff(k1, List.empty).toListL
      } yield {
        size1 shouldBe set1.size
        size2 shouldBe set2.size
        diff should contain theSameElementsAs List("a", "b")
        noDiff should contain theSameElementsAs set1
      }
    }
  }

  "sDiffStore" should "sDiffStore" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val kDiff: K = genRedisKey.sample.get
    val set1: List[V] = List("a", "b", "c", "d")
    val set2: List[V] = List("c", "d", "e")

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        size1 <- cmd.set.sAdd(k1, set1)
        size2 <- cmd.set.sAdd(k2, set2)
        diffSize <- cmd.set.sDiffStore(kDiff, k1, k2)
        all <- cmd.set.sDiffStore(".", k1, Set.empty)
        members <- cmd.set.sMembers(kDiff).toListL
      } yield {
        size1 shouldBe set1.size
        size2 shouldBe set2.size
        diffSize shouldBe 2
        all shouldBe set1.size
        members should contain theSameElementsAs List("a", "b")
      }
    }
  }

  "sInter" should "intersect multiple sets" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val set1: List[V] = List("a", "b", "c")
    val set2: List[V] = List("b", "c", "d")

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        size1 <- cmd.set.sAdd(k1, set1)
        size2 <- cmd.set.sAdd(k2, set2)
        intersec <- cmd.set.sInter(k1, k2).toListL
      } yield {
        size1 shouldBe set1.size
        size2 shouldBe set2.size
        intersec should contain theSameElementsAs List("b", "c")
      }
    }
  }

  "sInterStore" should "intersect multiple sets and store the resulting set in a key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val kDestination: K = genRedisKey.sample.get
    val set1: List[V] = List("a", "b", "c")
    val set2: List[V] = List("b", "c", "d")

    //when
    utfConnection.use[Task, Assertion] { cmd =>
      for {
        size1 <- cmd.set.sAdd(k1, set1)
        size2 <- cmd.set.sAdd(k2, set2)
        intersec <- cmd.set.sInterStore(kDestination, k1, k2)
        members <- cmd.set.sMembers(kDestination).toListL
      } yield {
        size1 shouldBe set1.size
        size2 shouldBe set2.size
        intersec shouldBe 2L
        members should contain theSameElementsAs List("b", "c")
      }
    }
  }

  "sIsMember" should "determine if a given value is a member of a set" in {
    val k: K = genRedisKey.sample.get
    val members: List[V] = List("a", "b", "c")

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.set.sAdd(k, members)
        isMember1 <- cmd.set.sIsMember(k, members.head)
        isMember2 <- cmd.set.sIsMember(k, genRedisValue.sample.get)
      } yield {
        isMember1 shouldBe true
        isMember2 shouldBe false
      }
    }
  }

  "sMove" should "move a member from one set to another" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val members: List[V] = List("a", "b", "c")

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.set.sAdd(k1, members)
        isMoved1 <- cmd.set.sMove(k1, k2, "b")
        isMoved2 <- cmd.set.sMove(k1, k2, "b")
        members1 <- cmd.set.sMembers(k1).toListL
        members2 <- cmd.set.sMembers(k2).toListL
      } yield {
        isMoved1 shouldBe true
        isMoved2 shouldBe false
        members1 should contain theSameElementsAs List("a", "c")
        members2 shouldBe List("b")
      }
    }
  }

  "sMembers" should "get all the members in a set" in {
    val k: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        size <- cmd.set.sAdd(k, values)
        members <- cmd.set.sMembers(k).toListL
      } yield {
        size shouldBe values.size
        members should contain theSameElementsAs values
      }
    }
  }

  "sPop" should "remove and return a random member from a set" in {
    val k: K = genRedisKey.sample.get
    val values: List[V] = List("a", "b", "c")

    utfConnection.use[Task, Assertion]  { cmd =>
      val popTask = cmd.set.sPop(k)
      for {
        size <- cmd.set.sAdd(k, values)
        popSequence <- Task.sequence(List.fill(3)(popTask))
        empty <- cmd.set.sPop(k)
      } yield {
        size shouldBe values.size
        popSequence should contain theSameElementsAs List(Some("a"), Some("b"), Some("c"))
        empty shouldBe None
      }
    }
  }

  "sRandMember" should "get one random member from a set" in {
    val k: K = genRedisKey.sample.get
    val values: List[V] = Gen.listOfN(1000, genRedisValue).sample.get

    utfConnection.use[Task, Assertion]  { cmd =>
      for {
        _ <- cmd.set.sAdd(k, values)
        rands <- cmd.set.sRandMember(k, 100).toListL
        members <- cmd.set.sMembers(k).toListL
        nonExistingKey <- cmd.set.sRandMember("non-existing-key")
      } yield {
        (rands.toSet.size > 10L) shouldBe true
        members should contain theSameElementsAs values.toSet
        nonExistingKey shouldBe None
      }
    }
  }

  it should "get one or multiple random members from a set" in {
    val k: K = genRedisKey.sample.get
    val values: List[V] = Gen.listOfN(1000, genRedisValue).sample.get

    utfConnection.use[Task, Assertion]  { cmd =>
      for {
        _ <- cmd.set.sAdd(k, values)
        rands <- Task.sequence(List.fill(1000)(cmd.set.sRandMember(k)))
        members <- cmd.set.sMembers(k).toListL
        nonExistingKey <- cmd.set.sRandMember("non-existing-key")
      } yield {
        val grouped = rands.groupBy(_.getOrElse("none"))
        (grouped.size > 10L) shouldBe true
        members should contain theSameElementsAs values.toSet
        nonExistingKey shouldBe None
      }
    }
  }

  "sRem" should "remove one or more members from a set" in {
    val k: K = genRedisKey.sample.get
    val values: List[V] = List("a", "b", "c")

    utfConnection.use[Task, Assertion]  { cmd =>
      for {
        size <- cmd.set.sAdd(k, values)
        remA <- cmd.set.sRem(k, "a")
        remBc <- cmd.set.sRem(k, List("b", "c"))
        empty <- cmd.set.sRem(k, "a", "b", "c")
        nonExistingKey <- cmd.set.sRem("non-existing-key", "a")
      } yield {
        size shouldBe values.size
        remA shouldBe 1L
        remBc shouldBe 2L
        empty shouldBe 0L
        nonExistingKey shouldBe 0L
      }
    }
  }

  "sUnion" should "add multiple sets" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val values1: List[V] = List("a", "b", "c")
    val values2: List[V] = List("c", "d", "e")

    utfConnection.use[Task, Assertion] { cmd =>
      for {
        _ <- cmd.set.sAdd(k1, values1) >> cmd.set.sAdd(k2, values2)
        union <- cmd.set.sUnion(k1, k2).toListL
        empty <- cmd.set.sUnion("non", "existing", "keys").toListL
      } yield {
        union should contain theSameElementsAs List("a", "b", "c", "d", "e")
        empty shouldBe List.empty
      }
    }
  }

  "sUnionStore" should "add multiple sets and store the resulting set in a key" in {
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val kDest: K = genRedisKey.sample.get
    val values1: List[V] = List("a", "b", "c")
    val values2: List[V] = List("c", "d", "e")

    utfConnection.use[Task, Assertion]  { cmd =>
      for {
        _ <- cmd.set.sAdd(k1, values1) >> cmd.set.sAdd(k2, values2)
        union <- cmd.set.sUnionStore(kDest, k1, k2)
        members <- cmd.set.sMembers(kDest).toListL
        empty <- cmd.set.sUnionStore(kDest, "non", "existing", "keys")
      } yield {
        union shouldBe (values1 ++ values2).toSet.size
        members should contain theSameElementsAs List("a", "b", "c", "d", "e")
        empty shouldBe 0L
      }
    }
  }

  it should "allow to compose nice for comprehensions" in {
    val k1: K = genRedisKey.sample.get
    val m1: List[String] = genRedisValues.sample.get
    val k2: K = genRedisKey.sample.get
    val m2: List[String] = genRedisValues.sample.get
    val k3: K = genRedisKey.sample.get

      utfConnection.use[Task, Assertion] { case RedisCmd(_, _, _, _, set, _, _) =>
        for {
          size1 <- set.sAdd(k1, m1: _*)
          size2 <- set.sAdd(k2, m2: _*)
          _ <- set.sAdd(k3, m1: _*)
          moved <- set.sMove(k1, k2, m1.head)
          s1 <- set.sMembers(k1).toListL
          s2 <- set.sMembers(k2).toListL
          union <- set.sUnion(k1, k2).toListL
          diff <- set.sDiff(k3, k1).toListL
        } yield {
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
    }

  }


}
