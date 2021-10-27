package monix.connect.sqs

import monix.connect.sqs.producer.Message
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskSpec
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class StandardQueueSuite extends AsyncFlatSpec with MonixTaskSpec with Matchers with BeforeAndAfterAll with SqsITFixture {

  implicit val scheduler = Scheduler.io("sqs-standard-queue-suite")

  "A standard queue" can "be created and receive messages" in {
    val message = genStandardMessage.sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName)
        messageResponse <- sqs.producer.sendSingleMessage(message, queueUrl)
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
  }

  it should "not support messages with groupId nor deduplicationId" in {

    val messageWithGroupId = new Message("dummyBody1", groupId = Some("someGroupId"), deduplicationId = None)
    val messageWithDeduplicationId = new Message("dummyBody2", groupId = None, deduplicationId = Some("123"))
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName)
        isInvalidGroupId <- sqs.producer.sendSingleMessage(messageWithGroupId, queueUrl).as(false)
          .onErrorHandle(ex => if (ex.getMessage.contains("MessageGroupId is invalid")) true else false)
        isInvalidDeduplicationId <- sqs.producer.sendSingleMessage(messageWithDeduplicationId, queueUrl)
          .onErrorHandle(ex => if (ex.getMessage.contains("MessageDeduplicationId is invalid")) true else false)
      } yield {
        isInvalidGroupId shouldBe true
        isInvalidDeduplicationId shouldBe true
      }
  }

  it should "respect delayDuration of messages " in {

    val delayedMessage = genStandardMessage.map(_.copy(delayDuration = Some(5.seconds))).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName)
        _ <- sqs.producer.sendSingleMessage(delayedMessage, queueUrl)
        receivedMessages1 <- Task.sleep(2.seconds) *> sqs.consumer.receiveSingleAutoDelete(queueUrl)
        receivedMessages2 <- Task.sleep(5.seconds) *> sqs.consumer.receiveSingleAutoDelete(queueUrl)
      } yield {
        receivedMessages1 shouldBe List.empty
        receivedMessages2.size shouldBe 1
        receivedMessages2.head.body shouldBe delayedMessage.body
      }
  }

  it should "not respect `maxMessages` and process more than 10 `inFlight`" in {
    val messages = Gen.listOfN(15, genStandardMessage).sample.get
     for {
        sqs <- unsafeSqsAsyncClient
        queueUrl <- sqs.operator.createQueue(queueName)
        _ <- Task.traverse(messages)(sqs.producer.sendSingleMessage(_, queueUrl))
        receivedMessages1 <- sqs.consumer.receiveSingleManualDelete(queueUrl, maxMessages = 1)
          .tapEval(Task.traverse(_)(_.deleteFromQueue()))
        receivedMessages2 <- sqs.consumer.receiveSingleManualDelete(queueUrl, maxMessages = 5)
        receivedMessages3 <- sqs.consumer.receiveSingleManualDelete(queueUrl)
      } yield {
        receivedMessages1.size shouldBe 1
        receivedMessages2.size shouldBe 5
        receivedMessages3.size shouldBe 9
        (receivedMessages1 ++ receivedMessages2 ++ receivedMessages3).map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

  it should "delete messages even after exceeding the visibility timeout" in {
    val messages = Gen.listOfN(10, genStandardMessage).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        queueUrl <- sqs.operator.createQueue(queueName)
        _ <- Task.traverse(messages)(sqs.producer.sendSingleMessage(_, queueUrl))
        receivedMessages1 <-
          sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.second)
            .tapEval(Task.parTraverse(_)(_.deleteFromQueue().attempt).delayExecution(2.seconds).startAndForget)
        receivedMessages2 <- Task.sleep(3.seconds) *>
          sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.second)
            .tapEval(Task.parTraverse(_)(_.deleteFromQueue().attempt).delayExecution(2.seconds).startAndForget)
        receivedMessages3 <- Task.sleep(3.seconds) *>
          sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.second)
            .tapEval(Task.parTraverse(_)(_.deleteFromQueue().attempt).delayExecution(2.seconds).startAndForget)
      } yield {
        receivedMessages1.size shouldBe 10
        receivedMessages2.size shouldBe 0
        receivedMessages3.size shouldBe 0
      }
  }

  it should "not delete messages if it gets consumed again before being deleted" in {
    val messages = Gen.listOfN(10, genStandardMessage).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        queueUrl <- sqs.operator.createQueue(queueName)
        _ <- Task.traverse(messages)(sqs.producer.sendSingleMessage(_, queueUrl))
        receivedMessages1 <-
          sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.second)
            .tapEval(Task.parTraverse(_)(_.deleteFromQueue().attempt).delayExecution(3.seconds).startAndForget)
        receivedMessages2 <- Task.sleep(2.seconds) *>
          sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.second)
            .tapEval(Task.parTraverse(_)(_.deleteFromQueue().attempt).delayExecution(4.seconds).startAndForget)
        receivedMessages3 <- Task.sleep(3.seconds) *>
          sqs.consumer.receiveSingleManualDelete(queueUrl, visibilityTimeout = 1.second)
      } yield {
        receivedMessages1.size shouldBe 10
        receivedMessages2.size shouldBe 10
        receivedMessages3.size shouldBe 10
      }
  }

  "A stream of received messages" can "be atomically consumed with automatic deletes" in {

    val n = 10
    val messages = Gen.listOfN(n, genStandardMessage).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName)
        _ <- Observable.fromIterable(messages)
          .consumeWith(sqs.producer.sendSink(queueUrl))
        receivedMessages <- sqs.consumer.receiveAutoDelete(queueUrl)
          .take(n).toListL
      } yield {
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

  it can "be consumed with manual deletes" in {

    val n = 10
    val messages = Gen.listOfN(n, genStandardMessage).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName)
        _ <- Observable.fromIterable(messages)
          .consumeWith(sqs.producer.sendSink(queueUrl))
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl)
          .mapEvalF(deletable => deletable.deleteFromQueue().as(deletable))
          .take(n).toListL
      } yield {
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

}
