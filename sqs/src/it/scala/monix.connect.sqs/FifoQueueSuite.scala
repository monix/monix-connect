package monix.connect.sqs

import cats.effect.Bracket
import monix.connect.sqs.producer.{Message, StandardMessage}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskSpec
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.{QueueAttributeName, SqsException}

import scala.concurrent.duration._

class FifoQueueSuite extends AsyncFlatSpec with MonixTaskSpec with Matchers with BeforeAndAfterEach with SqsITFixture {

  implicit val scheduler: Scheduler = Scheduler.io("sqs-fifo-queue-suite")

  val t: Bracket[Task, Throwable] = Bracket.apply

  "A fifo queue" can "be created and used to receive and produce messages" in {
    val message = genFifoMessage(defaultGroupId, deduplicationId = Some("123")).sample.get
    for {
      sqs <- Task(unsafeSqsAsyncClient)
      fifoQueueName <- Task.from(genFifoQueueName)
      queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
      messageResponse <- sqs.producer.sendSingleMessage(message, queueUrl)
      receivedMessage <- sqs.consumer.receiveSingleAutoDelete(queueUrl)
    } yield {
      messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      receivedMessage.size shouldBe 1
      receivedMessage.head.body shouldBe message.body
    }
  }

  it can "requires group id when a message is produced" in {
    val message = StandardMessage("body").asInstanceOf[Message]
      for {
        sqs <- Task(unsafeSqsAsyncClient)
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        isGroupIdRequired <- sqs.producer.sendSingleMessage(message, queueUrl)
          .onErrorHandle(ex => if (ex.getMessage.contains("The request must contain the parameter MessageGroupId")) true else false)
      } yield {
        isGroupIdRequired shouldBe true
      }
  }

  it can "requires either explicit deduplication ids or content based one" in {
    val messageWithoutDeduplicationId = genFifoMessage(defaultGroupId, deduplicationId = None).sample.get
      for {
        sqs <- Task(unsafeSqsAsyncClient)
        fifoQueueName <- Task.from(genFifoQueueName)
        manualDedupQueueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        failsWithoutDedupMechanism <- sqs.producer.sendSingleMessage(messageWithoutDeduplicationId, manualDedupQueueUrl)
          .onErrorHandle(ex => if (ex.getMessage.contains("The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly")) true else false)
      } yield {
        failsWithoutDedupMechanism shouldBe true
      }
  }

  it can "send messages to a fifo queue, with manual deduplication" in {
    val message1 = genFifoMessage(defaultGroupId, deduplicationId = Some(genId.sample.get)).sample.get
    val duplicatedMessageId1 = message1.copy(body = Gen.identifier.sample.get)
    val message2 = genFifoMessage(defaultGroupId, deduplicationId = Some(genId.sample.get)).sample.get
    val duplicatedMessageId2 = message1.copy(body = Gen.identifier.sample.get)

      for {
        sqs <- Task(unsafeSqsAsyncClient)
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response1 <- sqs.producer.sendSingleMessage(message1, queueUrl)
        duplicatedResponse1 <- sqs.producer.sendSingleMessage(duplicatedMessageId1, queueUrl)
        response2 <- sqs.producer.sendSingleMessage(message2, queueUrl)
        duplicatedResponse2 <- sqs.producer.sendSingleMessage(duplicatedMessageId2, queueUrl)
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl)
          .bufferTimed(2.seconds)
          .headL
      } yield {
        response1.md5OfMessageBody shouldBe md5Hex(message1.body)
        duplicatedResponse1.md5OfMessageBody shouldBe md5Hex(duplicatedMessageId1.body)
        response2.md5OfMessageBody shouldBe md5Hex(message2.body)
        duplicatedResponse2.md5OfMessageBody shouldBe md5Hex(duplicatedMessageId2.body)
        receivedMessages.map(_.body) should contain theSameElementsAs List(message1, message2).map(_.body)
      }
  }

  it can "send messages to fifo a queue, with content based deduplication" in {
    val message1 = genFifoMessage(defaultGroupId, deduplicationId = None).sample.get
    val message2 = genFifoMessage(defaultGroupId, deduplicationId = None).sample.get

      for {
        sqs <- Task(unsafeSqsAsyncClient)
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response1 <- sqs.producer.sendSingleMessage(message1, queueUrl)
        duplicatedResponse1 <- sqs.producer.sendSingleMessage(message1, queueUrl)
        _ <- sqs.producer.sendSingleMessage(message1, queueUrl)
        _ <- sqs.producer.sendSingleMessage(message1, queueUrl)
        response2 <- sqs.producer.sendSingleMessage(message2, queueUrl)
        duplicatedResponse2 <- sqs.producer.sendSingleMessage(message2, queueUrl)
        receivedMessages <- sqs.consumer.receiveAutoDelete(queueUrl)
          .bufferTimed(5.seconds)
          .headL
      } yield {
        response1.md5OfMessageBody shouldBe md5Hex(message1.body)
        duplicatedResponse1.md5OfMessageBody shouldBe md5Hex(message1.body)
        response2.md5OfMessageBody shouldBe md5Hex(message2.body)
        duplicatedResponse2.md5OfMessageBody shouldBe md5Hex(message2.body)
        receivedMessages.map(_.body) should contain theSameElementsAs List(message1, message2).map(_.body)
      }
  }

  it should "respect inflight messages on the same groupId even when having multiple consumers watching the same queue" in {
    val messages = Gen.listOfN(10, genFifoMessageWithDeduplication(defaultGroupId)).sample.get

      for {
        sqs <- Task(unsafeSqsAsyncClient)
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        _ <- Observable.fromIterable(messages).consumeWith(sqs.producer.sendSink(queueUrl))
        _ <- Observable.fromIterable(messages).consumeWith(sqs.producer.sendSink(queueUrl))
        result <- {
          val singleReceiveTask = sqs.consumer.receiveSingleManualDelete(queueUrl, maxMessages = 7)
          singleReceiveTask.flatMap(a => singleReceiveTask.map((a, _)))
        }
      } yield {
        result._1.size shouldBe 7
        result._2.size shouldBe 0
      }
  }

  "Explicit deduplication" should "take preference in front content based one" in {
    val message1 = genFifoMessage(defaultGroupId, deduplicationId = Some("111")).sample.get
    val body = message1.body
    val message2 = message1.copy(deduplicationId = Some("222"))
    val message3 = message1.copy(deduplicationId = Some("333"))
      for {
        sqs <- Task(unsafeSqsAsyncClient)
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response1 <- sqs.producer.sendSingleMessage(message1, queueUrl)
        response2 <- sqs.producer.sendSingleMessage(message2, queueUrl)
        response3 <- sqs.producer.sendSingleMessage(message3, queueUrl)
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl)
          .doOnNextF(_.deleteFromQueue())
          .bufferTimed(3.seconds).headL
      } yield {
        response1.md5OfMessageBody shouldBe md5Hex(body)
        response2.md5OfMessageBody shouldBe response1.md5OfMessageBody
        response3.md5OfMessageBody shouldBe response1.md5OfMessageBody
        receivedMessages.map(_.body) should contain theSameElementsAs List.fill(3)(body)
      }

  }

  "Delay duration" must "be invalid in fifo queues" in {
    val body: String = genId.sample.get
    val groupId = Gen.some(genGroupId).sample.get
    val deduplicationId = Gen.some(genId).sample.get
    val delayDuration = Some(5.seconds)
    val delayedMessage: Message = new Message(body, groupId = groupId, deduplicationId = deduplicationId, delayDuration = delayDuration)
    Sqs.fromConfig.use { case Sqs(operator, producer, _) =>
      for {
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        invalidRequest <- producer.sendSingleMessage(delayedMessage, queueUrl)
      } yield invalidRequest
    }.attempt.asserting { attempt =>
      attempt shouldBe a[Left[Throwable, _]]
      attempt.toTry.failed.get shouldBe a[SqsException]
      attempt.toTry.failed.get.getMessage.contains("DelaySeconds is invalid. The request include parameter that is not valid for this queue type") shouldBe true
    }
  }
}
