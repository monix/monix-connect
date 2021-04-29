package monix.connect.sqs

import monix.connect.sqs.domain.{InboundMessage, QueueName}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.QueueAttributeName

import scala.concurrent.duration._

class SqsFifoQueueSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get

  "A single message" can "be sent and received to fifo queue" in {
    val queueName = genFifoQueueName.sample.get
    val message = Gen.identifier.map(_.take(10)).map(id => InboundMessage(id, Some(id))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        messageResponse <- sqs.producer.sendSingleMessage(message, queueUrl, Some("groupId1"))
        received <- sqs.consumer.singleManualDelete(queueUrl)
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
    }.runSyncUnsafe()
  }

  it can "send messages to a fifo queue, with manual deduplication and no deletes" in {
    val queueName = genFifoQueueName.sample.get
    val groupId = "groupId"
    val deduplicationId1 = "deduplicationId1"
    val deduplicationId2 = "deduplicationId2"

    val message1 = genInboundMessage(Some(deduplicationId1)).sample.get
    val duplicatedMessageId1 = genInboundMessageWithDeduplication.sample.get.copy(deduplicationId = Some(deduplicationId1))
    val message2 = genInboundMessage(Some(deduplicationId2)).sample.get
    val duplicatedMessageId2 = genInboundMessageWithDeduplication.sample.get.copy(deduplicationId = Some(deduplicationId2))

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

  "A stream of messages" can "be produced and received in manual deletes mode" in {
    val groupId = "group123"
    val queueName = genFifoQueueName.sample.get
    val n = 15
    val messages = Gen.listOfN(n, genInboundMessageWithDeduplication).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(
          QueueAttributeName.FIFO_QUEUE -> "true"))
        _ <- Observable.fromIterable(messages).consumeWith(sqs.producer.sink(queueUrl, groupId = Some(groupId)))
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl)
          .mapEvalF(deletable => deletable.deleteFromQueue().as(deletable))
          .take(n)
          .toListL
      } yield {
        receivedMessages.size shouldBe n
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it can "be received in auto deleted mode" in {
    val queueName = genFifoQueueName.sample.get
    val n = 15
    val messages = Gen.listOfN(n, Gen.identifier.map(_.take(10)).map(id => InboundMessage(id, Some(id)))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        _ <- Observable.fromIterable(messages).consumeWith(sqs.producer.sink(queueUrl, groupId = Some("groupId123")))
        receivedMessages <- sqs.consumer.receiveAutoDelete(queueUrl)
          .take(n)
          .toListL
      } yield {
        receivedMessages.size shouldBe n
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it can "use content based deduplication" in {
    val groupId1 = "groupId1"
    val queueName = genQueueName.sample.get.map(_ + ".fifo") // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
    val message1 = Gen.identifier.map(_.take(10)).map(id => InboundMessage(id)).sample.get
    val message2 = Gen.identifier.map(_.take(10)).map(id => InboundMessage(id)).sample.get
    val message3 = Gen.identifier.map(_.take(10)).map(id => InboundMessage(id)).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes =
          Map(
            QueueAttributeName.FIFO_QUEUE -> "true",
            QueueAttributeName.CONTENT_BASED_DEDUPLICATION -> "true"))
        messageResponse1 <- sqs.producer.sendSingleMessage(message1, queueUrl, Some(groupId1))
        messageResponse2 <- sqs.producer.sendSingleMessage(message2, queueUrl, Some(groupId1))
        messageResponse3 <- sqs.producer.sendSingleMessage(message3, queueUrl, Some(groupId1))
        receivedMessages <- sqs.consumer.receiveManualDelete(queueUrl).doOnNextF(_.deleteFromQueue()).take(3).toListL
      } yield {
        messageResponse1.md5OfMessageBody shouldBe md5Hex(message1.body)
        //messageResponse11.md5OfMessageBody shouldBe md5Hex(message1.body)
        receivedMessages.map(_.body) should contain theSameElementsAs List(message1, message2, message3).map(_.body)
      }
    }.runSyncUnsafe()
  }

  override def beforeAll(): Unit = {
   // Task.from(asyncClient.createQueue(createQueueRequest(randomQueueName))).runSyncUnsafe()
    Thread.sleep(3000)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    //Task.from(client.deleteQueue(deleteQueueRequest("http://localhost:4576/queue/" + randomQueueName)))
    super.afterAll()
  }
}
