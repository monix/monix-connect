package monix.connect.redis

import monix.connect.redis.client.RedisCmd
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class SetCommandsSuite
  extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  "sAdd" should "add one or more members to a set" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        one <- cmd.set.sAdd(k1, values.head)
        size <- cmd.set.sAdd(k2, values: _*)
        members <- cmd.set.sMembers(k2).toListL
      } yield {
        //then
        one shouldBe 1L
        size shouldBe values.size
        members should contain theSameElementsAs values
      }
    }.runSyncUnsafe()
  }

  "sCard" should "get the number of members in a set" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        size <- cmd.set.sAdd(k, values: _*)
        card <- cmd.set.sCard(k)
      } yield {
        card shouldBe values.size
        card shouldBe size
      }
    }.runSyncUnsafe()
  }

  "sDiff" should "subtract the first set with the successive ones" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val set1: List[V] = List("a", "b", "c", "d")
    val set2: List[V] = List("c", "d", "e")

    //when
    utfConnection.use { cmd =>
      for {
        size1 <- cmd.set.sAdd(k1, set1)
        size2 <- cmd.set.sAdd(k2, set2)
        diff <- cmd.set.sDiff(k1, k2).toListL
        all <- cmd.set.sDiff(k1, List.empty).toListL
      } yield {
        size1 shouldBe set1.size
        size2 shouldBe set2.size
        diff shouldBe List("a", "b")
        all should contain theSameElementsAs set1
      }
    }.runSyncUnsafe()
  }

  "sDiffStore" should "sDiffStore" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val kDiff: K = genRedisKey.sample.get
    val set1: List[V] = List("a", "b", "c", "d")
    val set2: List[V] = List("c", "d", "e")

    //when
    utfConnection.use { cmd =>
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
        members shouldBe List("a", "b")
      }
    }.runSyncUnsafe()
  }

  "sInter" should "intersect multiple sets" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val set1: List[V] = List("a", "b", "c")
    val set2: List[V] = List("b", "c", "d")

    //when
    utfConnection.use { cmd =>
      for {
        size1 <- cmd.set.sAdd(k1, set1)
        size2 <- cmd.set.sAdd(k2, set2)
        intersec <- cmd.set.sInter(k1, k2).toListL
      } yield {
        size1 shouldBe set1.size
        size2 shouldBe set2.size
        intersec shouldBe List("b", "c")
      }
    }.runSyncUnsafe()
  }

  "sInterStore" should "sInterStore" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val kDestination: K = genRedisKey.sample.get
    val set1: List[V] = List("a", "b", "c")
    val set2: List[V] = List("b", "c", "d")

    //when
    utfConnection.use { cmd =>
      for {
        size1 <- cmd.set.sAdd(k1, set1)
        size2 <- cmd.set.sAdd(k2, set2)
        intersec <- cmd.set.sInterStore(kDestination, k1, k2)
        members <- cmd.set.sMembers(kDestination).toListL
      } yield {
        size1 shouldBe set1.size
        size2 shouldBe set2.size
        intersec shouldBe 2L
        members shouldBe List("b", "c")
      }
    }.runSyncUnsafe()
  }

  "sIsMember" should "determine if a given value is a member of a set" in {
    //given
    val k: K = genRedisKey.sample.get
    val members: List[V] = List("a", "b", "c")

    //when
    utfConnection.use { cmd =>
      for {
        size1 <- cmd.set.sAdd(k, members)
        isMember1 <- cmd.set.sIsMember(k, members.head)
        isMember2 <- cmd.set.sIsMember(k, genRedisValue.sample.get)
      } yield {
        isMember1 shouldBe true
        isMember2 shouldBe false
      }
    }.runSyncUnsafe()
  }

  "sMove" should "move a member from one set to another" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val members: List[V] = List("a", "b", "c")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.set.sAdd(k1, members)
        isMoved1 <- cmd.set.sMove(k1, k2, "b")
        isMoved2 <- cmd.set.sMove(k1, k2, "b")
        members1 <- cmd.set.sMembers(k1).toListL
        members2 <- cmd.set.sMembers(k2).toListL
      } yield {
        isMoved1 shouldBe true
        isMoved2 shouldBe false
        members1 shouldBe List("a", "c")
        members2 shouldBe List("b")
      }
    }.runSyncUnsafe()
  }

  "sMembers" should "get all the members in a set" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[V] = genRedisValues.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        size <- cmd.set.sAdd(k, values)
        members <- cmd.set.sMembers(k).toListL
      } yield {
        //then
        size shouldBe values.size
        members should contain theSameElementsAs values
      }
    }.runSyncUnsafe()
  }

  "sPop" should "remove and return a random member from a set" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[V] = List("a", "b", "c")

    //when
    utfConnection.use { cmd =>
      val popTask = cmd.set.sPop(k)
      for {
        size <- cmd.set.sAdd(k, values)
        popSequence <- Task.sequence(List.fill(3)(popTask))
        empty <- cmd.set.sPop(k)
      } yield {
        //then
        size shouldBe values.size
        popSequence should contain theSameElementsAs List(Some("a"), Some("b"), Some("c"))
        empty shouldBe None
      }
    }.runSyncUnsafe()
  }

  "sRandMember" should "get one random member from a set" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[V] = Gen.listOfN(1000, genRedisValue).sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.set.sAdd(k, values)
        rands <- cmd.set.sRandMember(k, 100).toListL
        members <- cmd.set.sMembers(k).toListL
        nonExistingKey <- cmd.set.sRandMember("non-existing-key")
      } yield {
        //then
        (rands.toSet.size > 10L) shouldBe true
        members should contain theSameElementsAs values.toSet
        nonExistingKey shouldBe None
      }
    }.runSyncUnsafe()
  }

  it should "get one or multiple random members from a set" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[V] = Gen.listOfN(1000, genRedisValue).sample.get

    //when
    utfConnection.use { cmd =>
      for {
        size <- cmd.set.sAdd(k, values)
        rands <- Task.sequence(List.fill(1000)(cmd.set.sRandMember(k)))
        members <- cmd.set.sMembers(k).toListL
        nonExistingKey <- cmd.set.sRandMember("non-existing-key")

      } yield {
        //then
        val grouped = rands.groupBy(_.getOrElse("none"))
        (grouped.size > 10L) shouldBe true
        members should contain theSameElementsAs values.toSet
        nonExistingKey shouldBe None
      }
    }.runSyncUnsafe()
  }

  "sRem" should "remove one or more members from a set" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[V] = List("a", "b", "c")

    //when
    utfConnection.use { cmd =>
      for {
        size <- cmd.set.sAdd(k, values)
        remA <- cmd.set.sRem(k, "a")
        remBc <- cmd.set.sRem(k, List("b", "c"))
        empty <- cmd.set.sRem(k, "a", "b", "c")
        nonExistingKey <- cmd.set.sRem("non-existing-key", "a")
      } yield {
        //then
        size shouldBe values.size
        remA shouldBe 1L
        remBc shouldBe 2L
        empty shouldBe 0L
        nonExistingKey shouldBe 0L
      }
    }.runSyncUnsafe()
  }

  "sUnion" should "add multiple sets" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val values1: List[V] = List("a", "b", "c")
    val values2: List[V] = List("c", "d", "e")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.set.sAdd(k1, values1) >> cmd.set.sAdd(k2, values2)
        union <- cmd.set.sUnion(k1, k2).toListL
        empty <- cmd.set.sUnion("non", "existing", "keys").toListL
      } yield {
        //then
        union should contain theSameElementsAs List("a", "b", "c", "d", "e")
        empty shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  "sUnionStore" should "sUnionStore" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val kDest: K = genRedisKey.sample.get
    val values1: List[V] = List("a", "b", "c")
    val values2: List[V] = List("c", "d", "e")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.set.sAdd(k1, values1) >> cmd.set.sAdd(k2, values2)
        union <- cmd.set.sUnionStore(kDest, k1, k2)
        members <- cmd.set.sMembers(kDest).toListL
        empty <- cmd.set.sUnionStore(kDest, "non", "existing", "keys")
      } yield {
        //then
        union shouldBe (values1 ++ values2).toSet.size
        members should contain theSameElementsAs List("a", "b", "c", "d", "e")
        empty shouldBe 0L
      }
    }.runSyncUnsafe()
  }

  it should "allow to compose nice for comprehensions" in {
    //given
    val k1: K = genRedisKey.sample.get
    val m1: List[String] = genRedisValues.sample.get
    val k2: K = genRedisKey.sample.get
    val m2: List[String] = genRedisValues.sample.get
    val k3: K = genRedisKey.sample.get

    //when
    val (size1, size2, moved) = {
      utfConnection.use { case RedisCmd(_, _, _, _, set, _, _) =>
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
      utfConnection.use { case RedisCmd(_, _, _, _, set, _, _) =>
        for {
          s1 <- set.sMembers(k1).toListL
          s2 <- set.sMembers(k2).toListL
          union <- set.sUnion(k1, k2).toListL
          diff <- set.sDiff(k3, k1).toListL
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


}
