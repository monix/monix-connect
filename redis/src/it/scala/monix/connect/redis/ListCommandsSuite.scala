package monix.connect.redis

import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.concurrent.duration._

class ListCommandsSuite
  extends AnyFlatSpec with RedisIntegrationFixture with Matchers with BeforeAndAfterEach with BeforeAndAfterAll
    with Eventually {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(4.seconds, 100.milliseconds)
  implicit val io = Scheduler.io()

  override def beforeEach(): Unit = {
    super.beforeEach()
    utfConnection.use(cmd => cmd.server.flushAll()).runSyncUnsafe()
  }

  it should "insert elements into a list and reading back the same elements" in {
    //given
    val key: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get

    //when
    utfConnection.use(_.list.lPush(key, values: _*)).runSyncUnsafe()

    //and
    val l: List[String] = utfConnection.use(_.list.lRange(key, 0, values.size).toListL).runSyncUnsafe()

    //then
    l should contain theSameElementsAs values
  }

  //todo test
   "bLPop" should "remove and get the first element in a list, or block until one is available" in {
   /* //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.lPush(k, values).delayExecution(3.seconds).startAndForget
        // blpop wait for a new value for 5 seconds, and the values are pushed after 1 seconds
        blPop <- cmd.list.bLPop(5.seconds, k)
        blPopEmpty <- cmd.list.bLPop(1.second, "non-existing-key")
      } yield {
        blPop shouldBe Some((k, Some(values.last)))
        blPopEmpty shouldBe None
      }
    }.runSyncUnsafe()*/
  }

  "bRPop" should "bRPop" in {}

  "bRPopLPush" should "bRPopLPush" in {}

  "lIndex" should "get an element from a list by its index" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = genRedisValues.sample.get

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.lPush(k, values)
        head <- cmd.list.lIndex(k, values.size - 1)
        last <- cmd.list.lIndex(k, 0)
        outOfRange <- cmd.list.lIndex(k, values.size)
      } yield {
        head shouldBe values.headOption
        last shouldBe values.lastOption
        outOfRange shouldBe None
      }
    }.runSyncUnsafe()
  }

  "lInsert" should "insert an element before or after another element in a list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C", "D", "E")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.lPush(k, values)
        pivotNotFound <- cmd.list.lInsert(k, before = true, "Z", "X")
        keyNotFound <- cmd.list.lInsert("non-existing-key", before = true, "Z", "X")
        size1 <- cmd.list.lInsert(k, before = true, "C", "BC")
        size2 <- cmd.list.lInsert(k, before = false, "C", "CD")
        range <- cmd.list.lRange(k, 0, size2).toListL
      } yield {
        size1 shouldBe values.size + 1
        size2 shouldBe size1 + 1
        keyNotFound shouldBe 0L
        pivotNotFound shouldBe -1L
        range should contain theSameElementsAs values:+("BC"):+("CD")
      }
    }.runSyncUnsafe()
  }

  "lInsertBefore" should "insert an element before the pivot element in the list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C", "D", "E")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.lPush(k, values)
        pivotNotFoundBefore <- cmd.list.lInsertBefore(k, "Z", "X")
        keyNotFoundBefore <- cmd.list.lInsertBefore("non-existing-key", "Z", "X")
        isInsertedBefore <- cmd.list.lInsertBefore(k, "C", "BC")
        range <- cmd.list.lRange(k, 0, values.size).toListL
      } yield {
        pivotNotFoundBefore shouldBe false
        keyNotFoundBefore shouldBe false
        isInsertedBefore shouldBe true
        range should contain theSameElementsAs values:+("BC")
      }
    }.runSyncUnsafe()
  }

  "lInsertAfter" should "insert an element before the pivot element in the list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C", "D", "E")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.lPush(k, values)
        pivotNotFoundAfter <- cmd.list.lInsertAfter(k, "Z", "X")
        keyNotFoundAfter <- cmd.list.lInsertAfter("non-existing-key", "Z", "X")
        isInsertedAfter <- cmd.list.lInsertAfter(k, "C", "CD")
        range <- cmd.list.lRange(k, 0, values.size).toListL
      } yield {
        pivotNotFoundAfter shouldBe false
        keyNotFoundAfter shouldBe false
        isInsertedAfter shouldBe true
        range should contain theSameElementsAs values:+("CD")
      }
    }.runSyncUnsafe()
  }

  "lPop" should "lPop" in {}

  "lPush" should "lPush" in {}

  "lPushX" should "lPushX" in {}

  "lRange" should "lRange" in {}

  "lRem" should "lRem" in {}

  "lSet" should "lSet" in {}

  "lTrim" should "lTrim" in {}

  "rPop" should "rPop" in {}

  "rPopLPush" should "rPopLPush" in {}

  "rPush" should "rPush" in {}

  "rPushX" should "rPushX" in {}

}
