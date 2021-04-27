package monix.connect.sqs

import monix.connect.sqs.domain.{QueueMessage, QueueName}
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

class SqsStandardQueueSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get

  // Standard queue
  "Sqs" can "send a single messages standard queue" in {
    val queueName = genQueueName.sample.get
    val message = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, None)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName)
        messageResponse <- sqs.sendMessage(message, queueUrl)

      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
    }.runSyncUnsafe()
  }

  it can "send and receive multiple messages standard queue" in {
    val queueName = genQueueName.sample.get
    val n = 10
    val messages = Gen.listOfN(n, Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, None))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName)
        _ <- Observable.fromIterable(messages)
          .consumeWith(sqs.sendMessageSink(queueUrl))
        receivedMessages <- sqs.receiveManualAck(queueUrl).take(n).toListL
      } yield {
        receivedMessages.map(_.message.body) should contain theSameElementsAs messages.map(_.body)
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
