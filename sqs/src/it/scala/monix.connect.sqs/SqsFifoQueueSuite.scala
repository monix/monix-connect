package monix.connect.sqs

import monix.connect.sqs.domain.{InboundMessage, QueueName}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
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

  "A single message" can "be sent and received to fifo queue" in {
    val queueName = genFifoQueueName.sample.get
    val message = genInboundMessageWithDeduplication.sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        messageResponse <- sqs.producer.sendSingleMessage(message, queueUrl, Some("groupId1"))
        receivedMessage <- sqs.consumer.receiveSingleAutoDelete(queueUrl)
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
        receivedMessage.size shouldBe 1
        receivedMessage.head.body shouldBe message.body
      }
    }.runSyncUnsafe()
  }

  it can "send messages to a fifo queue, with manual deduplication" in {
    val queueName = genFifoQueueName.sample.get
    val groupId = "groupId"

    val message1 = genInboundMessageWithDeduplication.sample.get
    val duplicatedMessageId1 = message1.copy(body = Gen.identifier.sample.get)
    val message2 = genInboundMessageWithDeduplication.sample.get
    val duplicatedMessageId2 = message1.copy(body = Gen.identifier.sample.get)

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response1 <- sqs.producer.sendSingleMessage(message1, queueUrl, Some(groupId))
        duplicatedResponse1 <- sqs.producer.sendSingleMessage(duplicatedMessageId1, queueUrl, Some(groupId))
        response2 <- sqs.producer.sendSingleMessage(message2, queueUrl, Some(groupId))
        duplicatedResponse2 <- sqs.producer.sendSingleMessage(duplicatedMessageId2, queueUrl, Some(groupId))
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

  it can "send messages to fifo queue, with content based deduplication" in {
    val queueName = genFifoQueueName.sample.get
    val groupId = "groupId1"
    val message1 = genInboundMessage(deduplicationId = None).sample.get
    val message2 = genInboundMessage(deduplicationId = None).sample.get
    val queueAttributes = Map(QueueAttributeName.FIFO_QUEUE -> "true",
      QueueAttributeName.CONTENT_BASED_DEDUPLICATION -> "true")
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = queueAttributes)
        response1 <- sqs.producer.sendSingleMessage(message1, queueUrl, Some(groupId))
        duplicatedResponse1 <- sqs.producer.sendSingleMessage(message1, queueUrl, Some(groupId))
        _ <- sqs.producer.sendSingleMessage(message1, queueUrl, Some(groupId))
        _ <- sqs.producer.sendSingleMessage(message1, queueUrl, Some(groupId))
        response2 <- sqs.producer.sendSingleMessage(message2, queueUrl, Some(groupId))
        duplicatedResponse2 <- sqs.producer.sendSingleMessage(message2, queueUrl, Some(groupId))
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
    val groupId1 = "groupId1"
    val queueName = genQueueName.sample.get.map(_ + ".fifo") // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
    val message1 = genInboundMessage(deduplicationId = Some("111111")).sample.get
    val body = message1.body
    val message2 = message1.copy(deduplicationId = Some("22222"))
    val message3 = message1.copy(deduplicationId = Some("33333"))

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes =
          Map(
            QueueAttributeName.FIFO_QUEUE -> "true",
            QueueAttributeName.CONTENT_BASED_DEDUPLICATION -> "true"))
        response1 <- sqs.producer.sendSingleMessage(message1, queueUrl, Some(groupId1))
        response2 <- sqs.producer.sendSingleMessage(message2, queueUrl, Some(groupId1))
        response3 <- sqs.producer.sendSingleMessage(message3, queueUrl, Some(groupId1))
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl).doOnNextF(_.deleteFromQueue()).bufferTimed(3.seconds).headL
      } yield {
        response1.md5OfMessageBody shouldBe md5Hex(body)
        response2.md5OfMessageBody shouldBe response1.md5OfMessageBody
        response3.md5OfMessageBody shouldBe response1.md5OfMessageBody
        receivedMessages.map(_.body) should contain theSameElementsAs List.fill(3)(body)
      }
    }.runSyncUnsafe()
  }

}
