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

class MixedCommandsSuite extends AnyFlatSpec
  with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

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
