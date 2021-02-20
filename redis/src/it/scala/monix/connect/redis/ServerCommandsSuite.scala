package monix.connect.redis

import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class ServerCommandsSuite
  extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  it should "insert a string into key and get its size from redis" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use(_.string.set(key, value)).runSyncUnsafe()
    val beforeFlush: Boolean = utfConnection.use(_.key.exists(key)).runSyncUnsafe()

    //and
    utfConnection.use(_.server.flushAll()).runSyncUnsafe()
    val afterFlush: Boolean = utfConnection.use(_.key.exists(key)).runSyncUnsafe()

    //then
    beforeFlush shouldEqual true
    afterFlush shouldEqual false
  }

}
