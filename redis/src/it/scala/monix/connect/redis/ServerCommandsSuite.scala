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
    utfConnection.use(cmd => cmd.server.flushAll).runSyncUnsafe()
  }

  it should "bgRewriteAOF" in {}

  it should "bgSave" in {}

  it should "clientGetName" in {}

  it should "clientSetName" in {}

  it should "clientKill" in {}

  it should "clientList" in {}

  it should "commandCount" in {}

  it should "configGet" in {}

  it should "configResetStat" in {}

  it should "flushAll" in {
    //given
    val key: K = genRedisKey.sample.get
    val value: String = genRedisValue.sample.get

    //when
    utfConnection.use(_.string.set(key, value)).runSyncUnsafe()
    val existsBeforeFlush: Boolean = utfConnection.use(_.key.exists(key)).runSyncUnsafe()

    //and
    utfConnection.use(_.server.flushAll).runSyncUnsafe()
    val existsAfterFlush: Boolean = utfConnection.use(_.key.exists(key)).runSyncUnsafe()

    //then
    existsBeforeFlush shouldEqual true
    existsAfterFlush shouldEqual false
  }

  it should "flushDb" in {}

}
