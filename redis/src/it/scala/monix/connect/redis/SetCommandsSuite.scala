package monix.connect.redis

import monix.connect.redis.client.RedisCmd
import monix.execution.Scheduler.Implicits.global
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
        all <- cmd.set.sDiffStore(".", k1, List.empty)
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

  "sInter" should "" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val kDiff: K = genRedisKey.sample.get
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

  "sInterStore" should "sInterStore" in {}

  "sIsMember" should "sIsMember" in {}

  "sMove" should "sMove" in {}

  "sMembers" should "sMembers" in {}

  "sPop" should "sPop" in {}

  "sRandMember" should "sRandMember" in {}

  it should "sRandMesmber" in {}

  "sRem" should "sRem" in {}

  "sUnion" should "sUnion" in {}

  "sUnionStore" should "sUnionStore" in {}

  "sScan" should "sScan" in {}

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
