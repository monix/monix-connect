package monix.connect.redis

import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.SchedulerService
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Ignore}

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

  //todo not yet supported
   "bLPop" should "remove and get the first element in a list, or block until one is available" in {
   /*  //given
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

  //todo not yet supported
  "bRPop" should "bRPop" in {
   /*
    //given
   val k: K = genRedisKey.sample.get
   val values: List[String] = genRedisValues.sample.get

   //when
   utfConnection.use { cmd =>
     for {
       _ <- cmd.list.rPush(k, values.head).delayExecution(1.seconds).startAndForget
       // blpop wait for a new value for 5 seconds, and the values are pushed after 1 seconds
       blPop2 <- cmd.list.bRPop(10, k)
       blPopEmpty <- cmd.list.bLPop(1.second, "non-existing-key")
     } yield {
       blPop2 shouldBe Some((k, Some(values.last)))
       blPopEmpty shouldBe None
     }
   }.runSyncUnsafe() */
  }

  //todo not yet supported
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
        range should contain theSameElementsAs values :+ ("BC") :+ ("CD")
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
        range should contain theSameElementsAs values :+ ("BC")
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
        range should contain theSameElementsAs values :+ ("CD")
      }
    }.runSyncUnsafe()
  }

  "lPop" should "remove and get the first element in a list." in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.lPush(k, values)
        a <- cmd.list.lPop(k)
        b <- cmd.list.lPop(k)
        c <- cmd.list.lPop(k)
        none <- cmd.list.lPop(k)
      } yield {
        a shouldBe Some("C")
        b shouldBe Some("B")
        c shouldBe Some("A")
        none shouldBe None
      }
    }.runSyncUnsafe()
  }

  "lPush" should "prepend one or multiple values to a list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C")

    //when
    utfConnection.use { cmd =>
      for {
        size <- cmd.list.lPush(k, values)
        elements <- cmd.list.lRange(k, 0, size).toListL
      } yield {
        size shouldBe values.size
        elements shouldBe values.reverse
      }
    }.runSyncUnsafe()
  }

  //todo it does not behaves as expected
  //"lPushX" should "prepend values to a list, only if the list exists" in {
  //  //given
  //  val k: K = genRedisKey.sample.get
  //  val values: List[String] = List("A", "B", "C")

  //  //when
  //  utfConnection.use { cmd =>
  //    for {
  //      first <- cmd.list.lPushX(k, values)
  //      // _ <- cmd.list.lPush(k, ".")
  //      //second <- cmd.list.lPushX(k, values)
  //      elements <- cmd.list.lRange(k, 0, values.size).toListL
  //    } yield {
  //      //first shouldBe 0L
  //      //second shouldBe values.size
  //      elements should contain theSameElementsAs values+:(".")
  //    }
  //  }.runSyncUnsafe()
  //}

  "lRange" should "get a range of elements from a list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C")

    //when
    utfConnection.use { cmd =>
      for {
        size <- cmd.list.lPush(k, values)
        elements1 <- cmd.list.lRange(k, 0, size).toListL
        elements2 <- cmd.list.lRange(k, -5, size + 5).toListL
      } yield {
        size shouldBe values.size
        elements1 should contain theSameElementsAs values
        elements2 should contain theSameElementsAs values
      }
    }.runSyncUnsafe()
  }


  "lGetAll" should "get a range of elements from a list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C")

    //when
    utfConnection.use { cmd =>
      for {
        elements <- cmd.list.lPush(k, values) >> cmd.list.lGetAll(k).toListL
        noElements <- cmd.list.lGetAll("non-existing-key").toListL
      } yield {
        elements.size shouldBe values.size
        elements should contain theSameElementsAs values
        noElements shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  "lRem" should "remove elements from a list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("a", "a", "a", "b")

    //when
    utfConnection.use { cmd =>
      for {
        size <- cmd.list.lPush(k, values)
        removed <- cmd.list.lRem(k, 2, "a")
        nonRemoved1 <- cmd.list.lRem(k, 2, "c")
        nonRemoved2 <- cmd.list.lRem(k, 2, "c")
        elements <- cmd.list.lRange(k, 0, size).toListL
      } yield {
        size shouldBe values.size
        removed shouldBe 2L
        nonRemoved1 shouldBe 0L
        nonRemoved2 shouldBe 0L
        elements should contain theSameElementsAs List("a", "b")
      }
    }.runSyncUnsafe()
  }

  "lSet" should "set the value of an element in a list by its index" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("a", "a", "a", "d")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.lPush(k, values)
        set1 <- cmd.list.lSet(k, 1, "b")
        set2 <- cmd.list.lSet(k, 2, "c")
        isIndexOutOfRange <- cmd.list.lSet(k, 10, "out-of-range")
        notExistingKey <- cmd.list.lSet("not-existing-key", 0, "out-of-range")
        elements <- cmd.list.lRange(k, 0, values.size).toListL
      } yield {
        set1 shouldBe true
        set2 shouldBe true
        isIndexOutOfRange shouldBe false
        notExistingKey shouldBe false
        elements should contain theSameElementsAs List("a", "b", "c", "d")
      }
    }.runSyncUnsafe()
  }

  "lTrim" should "trim a list to the specified range" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("a", "b", "c", "d", "e", "f", "g")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.lPush(k, values)
        _ <- cmd.list.lTrim(k, 1, 4)
        firstTrim <- cmd.list.lRange(k, 0, values.size).toListL
        _ <- cmd.list.lTrim(k, 0, 1)
        secondTrim <- cmd.list.lRange(k, 0, values.size).toListL
        _ <- cmd.list.lTrim(k, 0, 1)
        thirdTrim <- cmd.list.lRange(k, 0, values.size).toListL
        _ <- cmd.list.lTrim(k, 5, 10)
        fourthTrim <- cmd.list.lRange(k, 0, values.size).toListL
      } yield {
        firstTrim should contain theSameElementsAs List("f", "e", "d", "c")
        secondTrim should contain theSameElementsAs List("f", "e")
        thirdTrim should contain theSameElementsAs List("f", "e")
        fourthTrim should contain theSameElementsAs List.empty
      }
    }.runSyncUnsafe()
  }

  "rPop" should "remove and get the last element in a list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.rPush(k, values)
        a <- cmd.list.rPop(k)
        b <- cmd.list.rPop(k)
        c <- cmd.list.rPop(k)
        none <- cmd.list.rPop(k)
      } yield {
        a shouldBe Some("C")
        b shouldBe Some("B")
        c shouldBe Some("A")
        none shouldBe None
      }
    }.runSyncUnsafe()
  }

  "rPopLPush" should "remove the last element in a list, prepend it to another list and return it" in {
    //given
    val k1: K = genRedisKey.sample.get
    val k2: K = genRedisKey.sample.get
    val values: List[String] = List("a", "b", "c")

    //when
    utfConnection.use { cmd =>
      for {
        _ <- cmd.list.rPush(k1, values)
        c <- cmd.list.rPopLPush(k1, k2)
        b <- cmd.list.rPopLPush(k1, k2)
        a <- cmd.list.rPopLPush(k1, k2)
        d <- cmd.list.rPopLPush(k1, k2)
        elements1 <- cmd.list.lRange(k1, 0, values.size).toListL
        elements2 <- cmd.list.lRange(k2, 0, values.size).toListL
      } yield {
        c shouldBe Some("c")
        b shouldBe Some("b")
        a shouldBe Some("a")
        d shouldBe None
        elements1 shouldBe List.empty
        elements2 shouldBe values
      }
    }.runSyncUnsafe()
  }

  "rPush" should "append one or multiple values to a list" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C")

    //when
    utfConnection.use { cmd =>
      for {
        size <- cmd.list.rPush(k, values)
        elements <- cmd.list.lRange(k, 0, size).toListL
      } yield {
        size shouldBe values.size
        elements shouldBe values
      }
    }.runSyncUnsafe()
  }

  "rPushX" should "rPushX" in {
    //given
    val k: K = genRedisKey.sample.get
    val values: List[String] = List("A", "B", "C")

    //when
    utfConnection.use { cmd =>
      for {
        first <- cmd.list.rPushX(k, values)
        _ <- cmd.list.rPush(k, ".")
        second <- cmd.list.rPushX(k, values)
        elements <- cmd.list.lRange(k, 0, second).toListL
      } yield {
        first shouldBe 0L
        second shouldBe values.size + 1
        elements should contain theSameElementsAs (".") :: values
      }
    }.runSyncUnsafe()
  }

}
