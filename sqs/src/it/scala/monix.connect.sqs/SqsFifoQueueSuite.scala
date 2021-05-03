package monix.connect.sqs

import monix.connect.sqs.domain.{InboundMessage, QueueName, StandardMessage}
import monix.execution.Scheduler.Implicits.global
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.QueueAttributeName

import scala.concurrent.duration._

class SqsFifoQueueSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get

  "A fifo queue" can "be created and used to receive and produce messages" in {
    val queueName = genFifoQueueName.sample.get
    val message = genFifoMessage(defaultGroupId, deduplicationId = Some("123")).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        messageResponse <- sqs.producer.sendSingleMessage(message, queueUrl)
        receivedMessage <- sqs.consumer.receiveSingleAutoDelete(queueUrl)
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
        receivedMessage.size shouldBe 1
        receivedMessage.head.body shouldBe message.body
      }
    }.runSyncUnsafe()
  }

  it can "requires group id when a message is produced" in {
    val queueName = genFifoQueueName.sample.get
    val message = StandardMessage("body").asInstanceOf[InboundMessage]
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        isGroupIdRequired <- sqs.producer.sendSingleMessage(message, queueUrl)
          .onErrorHandle(ex => if(ex.getMessage.contains("The request must contain the parameter MessageGroupId")) true else false)
      } yield {
        isGroupIdRequired shouldBe true
      }
    }.runSyncUnsafe()
  }

  it can "requires either explicit deduplication ids or content based one" in {
    val messageWithoutDeduplicationId = genFifoMessage(defaultGroupId, deduplicationId = None).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        manualDedupQueueUrl <- sqs.operator.createQueue(genFifoQueueName.sample.get, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        failsWithoutDedupMechanism <- sqs.producer.sendSingleMessage(messageWithoutDeduplicationId, manualDedupQueueUrl)
          .onErrorHandle(ex => if(ex.getMessage.contains("The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly")) true else false)
      } yield {
        failsWithoutDedupMechanism shouldBe true
      }
    }.runSyncUnsafe()
  }

  it can "send messages to a fifo queue, with manual deduplication" in {
    val queueName = genFifoQueueName.sample.get
    val message1 = genFifoMessage(defaultGroupId, deduplicationId = Some(genId.sample.get)).sample.get
    val duplicatedMessageId1 = message1.copy(body = Gen.identifier.sample.get)
    val message2 = genFifoMessage(defaultGroupId, deduplicationId = Some(genId.sample.get)).sample.get
    val duplicatedMessageId2 = message1.copy(body = Gen.identifier.sample.get)

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
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
    }.runSyncUnsafe()
  }

  it can "send messages to fifo a queue, with content based deduplication" in {
    val queueName = genFifoQueueName.sample.get
    val message1 = genFifoMessage(defaultGroupId, deduplicationId = None).sample.get
    val message2 = genFifoMessage(defaultGroupId, deduplicationId = None).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = fifoDeduplicationQueueAttr)
        response1 <- sqs.producer.sendSingleMessage(message1, queueUrl)
        duplicatedResponse1 <- sqs.producer.sendSingleMessage(message1, queueUrl)
        _ <- sqs.producer.sendSingleMessage(message1, queueUrl)
        _ <- sqs.producer.sendSingleMessage(message1, queueUrl)
        response2 <- sqs.producer.sendSingleMessage(message2, queueUrl)
        duplicatedResponse2 <- sqs.producer.sendSingleMessage(message2, queueUrl)
        receivedMessages <- sqs.consumer.receiveAutoDelete(queueUrl)
          .bufferTimed(2.seconds)
          .headL
      } yield {
        response1.md5OfMessageBody shouldBe md5Hex(message1.body)
        duplicatedResponse1.md5OfMessageBody shouldBe md5Hex(message1.body)
        response2.md5OfMessageBody shouldBe md5Hex(message2.body)
        duplicatedResponse2.md5OfMessageBody shouldBe md5Hex(message2.body)
        receivedMessages.map(_.body) should contain theSameElementsAs List(message1, message2).map(_.body)
      }
    }.runSyncUnsafe()
  }

  "Explicit deduplication" should "take preference in front content based one" in {
    val queueName = genFifoQueueName.sample.get
    val message1 = genFifoMessage(defaultGroupId, deduplicationId = Some("111")).sample.get
    val body = message1.body
    val message2 = message1.copy(deduplicationId = Some("222"))
    val message3 = message1.copy(deduplicationId = Some("333"))
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = fifoDeduplicationQueueAttr)
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
    }.runSyncUnsafe()
  }

}
