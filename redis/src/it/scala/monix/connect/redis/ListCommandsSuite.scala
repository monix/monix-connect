package monix.connect.redis

import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class ListCommandsSuite
  extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  it should "insert elements into a list and reading back the same elements" in {
    //given
    val key: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get.map(_.toString)

    //when
    utfConnection.use(_.list.lPush(key, values: _*)).runSyncUnsafe()

    //and
    val l: List[String] = utfConnection.use(_.list.lRange(key, 0, values.size).toListL).runSyncUnsafe()

    //then
    l should contain theSameElementsAs values
  }

  it should "bLPop" in {}

  it should "bRPop" in {}

  it should "bRPopLPush" in {}

  it should "lIndex" in {}

  it should "lInsert" in {}

  it should "lLen" in {}

  it should "lPop" in {}

  it should "lPush" in {}

  it should "lPushX" in {}

  it should "lRange" in {}

  it should "lRem" in {}

  it should "lSet" in {}

  it should "lTrim" in {}

  it should "rPop" in {}

  it should "rPopLPush" in {}

  it should "rPush" in {}

  it should "rPushX" in {}

}
