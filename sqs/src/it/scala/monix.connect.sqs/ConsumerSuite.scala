package monix.connect.sqs

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ConsumerSuite extends AsyncFlatSpecLike with MonixTaskTest with Matchers with ScalaFutures with BeforeAndAfterAll with SqsITFixture {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  override implicit def scheduler: Scheduler = Scheduler.io("sqs-consumer-suite")

  "receiveSingleManualDelete" should "respect `inFlightMessages` and process up to 10 messages at a time" in {
    val messages = Gen.listOfN(15, genFifoMessage(defaultGroupId)).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        fifoQueueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- Task.traverse(messages)(sqs.producer.sendSingleMessage(_, fifoQueueUrl))
        receivedMessages1 <- sqs.consumer.receiveSingleManualDelete(fifoQueueUrl, maxMessages = 1)
          .tapEval(Task.traverse(_)(_.deleteFromQueue()))
        receivedMessages10 <- sqs.consumer.receiveSingleManualDelete(fifoQueueUrl, maxMessages = 10)
          .tapEval(Task.traverse(_)(_.deleteFromQueue()))
        receivedMessages4 <- sqs.consumer.receiveSingleManualDelete(fifoQueueUrl, maxMessages = 10)
          .tapEval(Task.traverse(_)(_.deleteFromQueue()))
        attemptReceiveMoreThan10 <- sqs.consumer.receiveSingleManualDelete(fifoQueueUrl, maxMessages = 11).attempt
      } yield {
        receivedMessages1.size shouldBe 1
        receivedMessages10.size shouldBe 10
        receivedMessages4.size shouldBe 4
        attemptReceiveMoreThan10.isLeft shouldBe true
        attemptReceiveMoreThan10.toTry.failed.get.getMessage should include("ReadCountOutOfRange")
        (receivedMessages1 ++ receivedMessages10 ++ receivedMessages4).map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

  it should "respect the specified `inFlight` messages on the same consumer or visibility timeout being exceeded" in {
    val messages = Gen.listOfN(15, genFifoMessage(defaultGroupId)).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- Task.traverse(messages)(sqs.producer.sendSingleMessage(_, queueUrl))
        receivedMessages1 <- sqs.consumer.receiveSingleManualDelete(queueUrl, maxMessages = 10, visibilityTimeout = 15.seconds)
          .tapEval(Task.traverse(_)(_.deleteFromQueue()).delayExecution(2.seconds).startAndForget)
        //the following receive calls will return empty since the previous messages were not yet processed and deleted
        inFlightMessagesReached1 <- sqs.consumer.receiveSingleManualDelete(queueUrl)
        inFlightMessagesReached2 <- Task.sleep(500.millis) >> sqs.consumer.receiveSingleManualDelete(queueUrl)
        receivedMessages2 <- Task.sleep(3.seconds) >> sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.second)
          .tapEval(Task.traverse(_)(_.deleteFromQueue()).delayExecution(3.seconds).startAndForget)
        // here already we consumed all messages, but since the deletion of the previous consumption
        // took longer than the visibility timeout, the next messages will be consumed again.
        receivedMessages3 <- Task.sleep(2.seconds) >> sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 15.seconds)
          .tapEval(Task.traverse(_)(_.deleteFromQueue()))
      } yield {
        receivedMessages1.size shouldBe 10
        inFlightMessagesReached1 shouldBe List.empty
        inFlightMessagesReached2 shouldBe List.empty
        receivedMessages2.size shouldBe 5
        receivedMessages3.size shouldBe 5
        (receivedMessages1 ++ receivedMessages2 ++ receivedMessages3).map(_.body).toSet should contain theSameElementsAs messages.map(_.body)
        receivedMessages2.map(_.body) should contain theSameElementsAs receivedMessages3.map(_.body)
      }
  }

  it should "not respect the specified `inFlight` messages in standard queues" in {
    val messages = Gen.listOfN(100, genStandardMessage).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        queueUrl <- sqs.operator.createQueue(queueName)
        _ <- Task.traverse(messages)(sqs.producer.sendSingleMessage(_, queueUrl))
        receivedAllMessages <- sqs.consumer.receiveManualDelete(queueUrl, maxMessages = 10, visibilityTimeout = 10.seconds).doOnNext(_.deleteFromQueue())
          .bufferTimedAndCounted(5.seconds, 100).headL
        noMoreMessages <- sqs.consumer.receiveSingleManualDelete(queueUrl)
      } yield {
        receivedAllMessages.size shouldBe messages.size
        noMoreMessages shouldBe List.empty
        receivedAllMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

  it should "avoid consuming the same message multiple times when `deleteFromQueue` is triggered and `visibilityTimeout` is exceeded" in {
    val message = genFifoMessage(defaultGroupId).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- sqs.producer.sendSingleMessage(message, queueUrl)
        receivedMessage <- sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.seconds)
          .tapEval(Task.traverse(_)(_.deleteFromQueue()))
        receivedMessagesAfterDeleteFromQueue <- Task.sleep(3.seconds) >> sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 10.seconds)
      } yield {
        receivedMessage.size shouldBe 1
        receivedMessage.map(_.body) shouldBe List(message.body)
        receivedMessagesAfterDeleteFromQueue shouldBe List.empty
      }
  }

  it can "consume the same message multiple times when `deleteFromQueue` is not triggered and `visibilityTimeout` is exceeded" in {
    val message = genFifoMessage(defaultGroupId).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- sqs.producer.sendSingleMessage(message, queueUrl)
        receivedMessage <- sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.seconds)
        duplicatedMessage <- Task.sleep(3.seconds) >> sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 10.seconds)
        emptyUnderVisibilityTimeout <- sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 5.seconds)
      } yield {
        receivedMessage.size shouldBe 1
        receivedMessage.map(_.body) shouldBe List(message.body)
        duplicatedMessage.size shouldBe 1
        duplicatedMessage.map(_.body) shouldBe List(message.body)
        emptyUnderVisibilityTimeout shouldBe List.empty
      }
  }

  it should "wait as indicated in `waitTimeSeconds` until a message arrives" in {
    val message1 = genFifoMessage(defaultGroupId).sample.get
    val message2 = genFifoMessage(defaultGroupId).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        receiveFiber <- sqs.consumer.receiveSingleManualDelete(queueUrl, waitTimeSeconds = 5.seconds).start
        _ <- Task.sleep(3.seconds) >> sqs.producer.sendSingleMessage(message1, queueUrl)
        receivedMessages <- receiveFiber.join
        receiveFiberEmpty <- sqs.consumer.receiveSingleManualDelete(queueUrl, waitTimeSeconds = 1.seconds).start
        _ <- Task.sleep(3.seconds) >> sqs.producer.sendSingleMessage(message2, queueUrl)
        receivedMessagesEmpty <- receiveFiberEmpty.join
      } yield {
        receivedMessages.size shouldBe 1
        receivedMessages.map(_.body) shouldBe List(message1.body)
        receivedMessagesEmpty shouldBe List.empty
      }
  }

  "receiveSingleAutoDelete" should "consume up to 10 messages at a time" in {
    val messages = Gen.listOfN(15, genFifoMessage(defaultGroupId)).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- Task.traverse(messages)(sqs.producer.sendSingleMessage(_, queueUrl))
        receivedMessages1 <- sqs.consumer.receiveSingleAutoDelete(queueUrl)
        receivedMessages2 <- sqs.consumer.receiveSingleAutoDelete(queueUrl)
        receivedMessages3 <- sqs.consumer.receiveSingleAutoDelete(queueUrl)
        attemptReceiveMoreThan10 <- sqs.consumer.receiveSingleManualDelete(queueUrl, maxMessages = 11).attempt
              } yield {
        receivedMessages1.size shouldBe 10
        receivedMessages2.size shouldBe 5
        receivedMessages3.size shouldBe 0
        attemptReceiveMoreThan10.isLeft shouldBe true
        attemptReceiveMoreThan10.toTry.failed.get.getMessage should include ("ReadCountOutOfRange")
        (receivedMessages1 ++ receivedMessages2 ++ receivedMessages3).map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

  it should "fail when consuming more than 10 messages at time" in {
    val messages = Gen.listOfN(15, genFifoMessage(defaultGroupId)).sample.get
    for {
      sqs <- unsafeSqsAsyncClient
      fifoQueueName <- Task.from(genFifoQueueName)
      queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
      _ <- Task.traverse(messages)(sqs.producer.sendSingleMessage(_, queueUrl))
      receiveAttempt <- sqs.consumer.receiveSingleAutoDelete(queueUrl, maxMessages = 11).attempt
    } yield {
      receiveAttempt.isLeft shouldBe true
      receiveAttempt.toTry.failed.get.getMessage should include ("ReadCountOutOfRange")
    }
  }

  it should "avoid consuming the same message multiple times with short `visibilityTimeout`, since messages are already deleted " in {
    val message = genFifoMessage(defaultGroupId).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- sqs.producer.sendSingleMessage(message, queueUrl)
        receivedMessage <- sqs.consumer.receiveSingleAutoDeleteInternal(queueUrl, visibilityTimeout = 1.seconds, waitTimeSeconds = 5.seconds, maxMessages = 10)
      } yield {
        receivedMessage.size shouldBe 1
        receivedMessage.map(_.body) shouldBe List(message.body)
      }
  }

  it should "wait as indicated in `waitTimeSeconds` until a message arrives" in {
    val message1 = genFifoMessage(defaultGroupId).sample.get
    val message2 = genFifoMessage(defaultGroupId).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        receiveFiber <- sqs.consumer.receiveSingleAutoDelete(queueUrl, waitTimeSeconds = 5.seconds).start
        _ <- Task.sleep(3.seconds) >> sqs.producer.sendSingleMessage(message1, queueUrl)
        receivedMessages <- receiveFiber.join
        receiveFiberEmpty <- sqs.consumer.receiveSingleAutoDelete(queueUrl, waitTimeSeconds = 1.seconds).start
        _ <- Task.sleep(3.seconds) >> sqs.producer.sendSingleMessage(message2, queueUrl)
        receivedMessagesEmpty <- receiveFiberEmpty.join
      } yield {
        receivedMessages.size shouldBe 1
        receivedMessages.map(_.body) shouldBe List(message1.body)
        receivedMessagesEmpty shouldBe List.empty
      }
  }

  "receiveManualDelete" can "allows to manually trigger deletes" in {
    val messages = Gen.choose(10, 150).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- Observable.fromIterable(messages).mapEvalF(sqs.producer.sendSingleMessage(_, queueUrl)).completedL
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl)
          .mapEvalF(deletable => deletable.deleteFromQueue().as(deletable))
          .bufferTimedAndCounted(5.seconds, messages.size)
          .headL
      } yield {
        receivedMessages.size shouldBe messages.size
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

  it should "consume the same message multiple times delete is not triggered" in {
    val messages = Gen.choose(1, 5).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- Observable.fromIterable(messages).mapEvalF(sqs.producer.sendSingleMessage(_, queueUrl)).completedL
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl, visibilityTimeout = 1.seconds)
          .bufferTimed(5.seconds)
          .headL
      } yield {
        receivedMessages.size should be > messages.size
      }
  }

  "receiveAutoDelete" can "does not require manually triggering message deletion" in {
    val messages = Gen.choose(10, 150).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- Observable.fromIterable(messages).mapEvalF(sqs.producer.sendSingleMessage(_, queueUrl)).completedL
        receivedMessages <- sqs.consumer.receiveAutoDelete(queueUrl)
          .bufferTimedAndCounted(10.seconds, messages.size)
          .headL
      } yield {
        receivedMessages.size shouldBe messages.size
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

  it should "NOT consume the same message multiple times since it's already been deleted" in {
    val messages = Gen.choose(1, 5).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- Observable.fromIterable(messages).mapEvalF(sqs.producer.sendSingleMessage(_, queueUrl)).completedL
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl, visibilityTimeout = 1.seconds)
          .bufferTimed(6.seconds)
          .headL
      } yield {
        receivedMessages.size should be > messages.size
      }
  }

}
