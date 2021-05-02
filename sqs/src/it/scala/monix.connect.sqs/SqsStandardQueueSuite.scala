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

class SqsStandardQueueSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get

  "A standard queue" can "be created and receive a single message" in {
    val queueName = genQueueName.sample.get
    val message = Gen.identifier.map(_.take(10)).map(id => InboundMessage(id, None)).sample.get
    Sqs.fromConfig.use { case Sqs(_, producer, operator) =>
      for {
        queueUrl <- operator.createQueue(queueName)
        messageResponse <- producer.sendSingleMessage(message, queueUrl)
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
    }.runSyncUnsafe()
  }

  "A stream of received messages" can "be atomically consumed with automatic deletes" in {
    val queueName = genQueueName.sample.get
    val n = 10
    val messages = Gen.listOfN(n, genInboundMessage(None)).sample.get
    Sqs.fromConfig.use { case Sqs(receiver, producer, operator) =>
      for {
        queueUrl <- operator.createQueue(queueName)
        _ <- Observable.fromIterable(messages)
          .consumeWith(producer.sink(queueUrl))
        receivedMessages <- receiver.receiveAutoDelete(queueUrl)
          .take(n).toListL
      } yield {
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it can "be consumed with manual deletes" in {
    val queueName = genQueueName.sample.get
    val n = 10
    val messages = Gen.listOfN(n, Gen.identifier.map(_.take(10)).map(id => InboundMessage(id, None))).sample.get
    Sqs.fromConfig.use { case Sqs(receiver, producer, operator) =>
      for {
        queueUrl <- operator.createQueue(queueName)
        _ <- Observable.fromIterable(messages)
          .consumeWith(producer.sink(queueUrl))
        receivedMessages <- receiver.receiveManualDelete(queueUrl).doOnNextF(deletable => deletable.deleteFromQueue().as())
          .take(n).toListL
      } yield {
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

}
