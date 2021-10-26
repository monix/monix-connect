package monix.connect.redis

import monix.connect.redis.client.RedisCmd
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class MixedCommandsSuite extends AsyncFlatSpec with MonixTaskSpec
  with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  override implicit val scheduler: Scheduler = Scheduler.io("mixed-commands-suite")

  "Redis" should "allow to composition of different Redis submodules" in {
    val k1: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get
    val k2: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get
    val k3: K = genRedisKey.sample.get

      utfConnection.use[Task, Assertion] { case RedisCmd(_, keys, list, server, _, _, string) =>
        for {
          _ <- server.flushDb
          _ <- keys.touch(k1)
          _ <- string.set(k1, value)
          _ <- keys.rename(k1, k2)
          _ <- list.lPush(k3, values: _*)
          v <- string.get(k2)
          _ <- v match {
            case Some(value) => list.lPush(k3, value)
            case None => Task.unit
          }
          _ <- keys.del(k2)
          len <- list.lLen(k3)
          l <- list.lRange(k3, 0, -1).toListL
          keys <- keys.keys("*").toListL // unsafe operation
        } yield {
          v shouldBe Some(value)
          len shouldBe values.size + 1
          l should contain theSameElementsAs value :: values
          keys.size shouldBe 1
          keys.head shouldBe k3
        }
    }
  }

}
