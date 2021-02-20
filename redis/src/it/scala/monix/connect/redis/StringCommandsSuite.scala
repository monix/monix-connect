package monix.connect.redis

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class StringCommandsSuite
  extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  it should "insert a string into the given key and get its size from redis" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use(_.string.set(key, value)).runSyncUnsafe()

    //and
    val t: Task[Long] = utfConnection.use(_.string.strLen(key))

    //then
    val lenght: Long = t.runSyncUnsafe()
    lenght shouldBe value.length
  }

}
