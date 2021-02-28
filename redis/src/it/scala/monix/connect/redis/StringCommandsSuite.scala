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

  "append" should "append a value to a key" in {
    //given
    val k1: K = genRedisKey.sample.get
    val v1: String = genRedisValue.sample.get
    val v2: String = genRedisValue.sample.get

    //when
    utfConnection.use { cmd =>
     for {
     append1 <- cmd.string.append(k1, v1)
     append2 <- cmd.string.set(k1, v1) >> cmd.string.append(k1, v2)
     } yield {
       append1 shouldBe 0L
       append2 shouldBe v1.length + v2.length
     }

    }.runSyncUnsafe()
  }

  it should "bitCount" in {}

  it should "bitPos" in {}

  it should "bitOpAnd" in {}

  it should "bitOpNot" in {}

  it should "bitOpOr" in {}

  it should "bitOpXor" in {}

  it should "decr" in {}

  it should "decrBy" in {}

  it should "get" in {}

  it should "getBit" in {}

  it should "getRange" in {}

  it should "getSet" in {}

  it should "incr" in {}

  it should "incrBy" in {}

  it should "incrByFloat" in {}

  it should "mGet" in {}

  it should "mSet" in {}

  it should "mSetNx" in {}

  it should "set" in {}

  it should "setBit" in {}

  it should "setEx" in {}

  it should "pSetEx" in {}

  it should "setNx" in {}

  it should "setRange" in {}

  it should "strLen" in {}

}
