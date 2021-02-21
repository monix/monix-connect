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

  it should "sAdd" in {}

  it should "sCard" in {}

  it should "sDiff" in {}

  it should "sDiffStore" in {}

  it should "sInter" in {}

  it should "sInterStore" in {}

  it should "sIsMember" in {}

  it should "sMove" in {}

  it should "sMembers" in {}

  it should "sPop" in {}

  it should "sRandMember" in {}

  it should "sRandMember" in {}

  it should "sRem" in {}

  it should "sUnion" in {}

  it should "sUnionStore" in {}

  it should "sScan" in {}

  it should "allow to compose nice for comprehensions" in {
    //given
    val k1: K = genRedisKey.sample.get
    val m1: List[String] = genRedisValues.sample.get
    val k2: K = genRedisKey.sample.get
    val m2: List[String] = genRedisValues.sample.get
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



}
