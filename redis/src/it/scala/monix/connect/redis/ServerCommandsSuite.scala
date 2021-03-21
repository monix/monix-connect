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

  //todo write tests
  it should "bgRewriteAOF" in {}

  //todo write tests
  it should "bgSave" in {}

  //todo write tests
  it should "clientGetName" in {}

  //todo write tests
  it should "clientSetName" in {}

  //todo write tests
  it should "clientKill" in {}

  //todo write tests
  it should "clientList" in {}

  //todo write tests
  it should "commandCount" in {}

  //todo write tests
  it should "configGet" in {}

  //todo write tests
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

  //todo write tests
  it should "flushDb" in {}

}
