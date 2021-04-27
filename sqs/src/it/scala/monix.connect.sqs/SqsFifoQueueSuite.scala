package monix.connect.sqs

import monix.connect.sqs.domain.{QueueMessage, QueueName, QueueUrl}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.commons.codec.digest.DigestUtils.{md2Hex, md5Hex}
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

// FIFO
  it can "send messages to fifo queue, with deduplication and group id" in {
    val queueName = genQueueName.sample.get.map(_ + ".fifo") // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
    val message = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, Some(id))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        messageResponse <- sqs.sendMessage(message, queueUrl, Some("groupId1"))
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
    }.runSyncUnsafe()
  }

  it can "send and receive multiple message in manual acknowledge mode" in {
    val queueName = genQueueName.sample.get.map(_ + ".fifo")
    val n = 15
    val messages = Gen.listOfN(n, Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, Some(id)))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        _ <- Observable.fromIterable(messages).consumeWith(sqs.sendMessageSink(queueUrl, groupId = Some("groupId123")))
        receivedMessages <- sqs.receiveManualAck(queueUrl)
          .doOnNextF(_.deleteFromQueue())
          .take(n)
          .toListL
      } yield {
        receivedMessages.map(_.message.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it can "send and receive multiple message in automatic acknowledge mode" in {
    val queueName = genQueueName.sample.get.map(_ + ".fifo")
    val n = 15
    val messages = Gen.listOfN(n, Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, Some(id)))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        _ <- Observable.fromIterable(messages).consumeWith(sqs.sendMessageSink(queueUrl, groupId = Some("groupId123")))
        receivedMessages <- sqs.receiveAutoAck(queueUrl)
          .take(n)
          .toListL
      } yield {
        receivedMessages.map(_.message.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it can "send messages to fifo queue, with auto deduplication enabled" in {
    val queueName = genQueueName.sample.get.map(_ + ".fifo") // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
    val message1 = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, Some(id))).sample.get
    val message2 = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, Some(id))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response1 <- sqs.sendMessage(message1, queueUrl, Some("groupId1"))
        duplicatedResponse1 <- sqs.sendMessage(message1, queueUrl, Some("groupId1"))
        response2 <- sqs.sendMessage(message2, queueUrl, Some("groupId1"))
      } yield {
        response1.md5OfMessageBody shouldBe md5Hex(message1.body)
        duplicatedResponse1.md5OfMessageBody shouldBe md5Hex(message1.body)
        response2.md5OfMessageBody shouldBe md5Hex(message2.body)
      }
    }.runSyncUnsafe()
  }


  it can "use content based deduplication" in {
    val groupId1 = "groupId1"
    val queueName = genQueueName.sample.get.map(_ + ".fifo") // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
    val message1 = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id)).sample.get
    val message2 = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id)).sample.get
    val message3 = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id)).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName, attributes =
          Map(
            QueueAttributeName.FIFO_QUEUE -> "true",
            QueueAttributeName.CONTENT_BASED_DEDUPLICATION -> "true"))
        messageResponse1 <- sqs.sendMessage(message1, queueUrl, Some(groupId1))
        messageResponse2 <- sqs.sendMessage(message2, queueUrl, Some(groupId1))
        messageResponse3 <- sqs.sendMessage(message3, queueUrl, Some(groupId1))
        receivedMessages <- sqs.receiveManualAck(queueUrl).doOnNextF(_.deleteFromQueue()).take(3).toListL
      } yield {
        messageResponse1.md5OfMessageBody shouldBe md5Hex(message1.body)
        //messageResponse11.md5OfMessageBody shouldBe md5Hex(message1.body)
        receivedMessages.map(_.message.body) should contain theSameElementsAs List(message1, message2, message3).map(_.body)
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
