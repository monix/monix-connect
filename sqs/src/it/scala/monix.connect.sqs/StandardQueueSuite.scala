package monix.connect.sqs

import monix.connect.sqs.inbound.InboundMessage
import monix.connect.sqs.domain.QueueName
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class StandardQueueSuite extends AnyFlatSpecLike with Matchers with BeforeAndAfterEach with SqsITFixture {

  "A standard queue" can "be created and receive messages" in {
    val message = genStandardMessage.sample.get
    Sqs.fromConfig.use { case Sqs(_, producer, operator) =>
      for {
        queueUrl <- operator.createQueue(fifoQueueName)
        messageResponse <- producer.sendSingleMessage(message, queueUrl)
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
    }.runSyncUnsafe()
  }

  it should "not support messages with groupId nor deduplicationId" in {

    val messageWithGroupId = new InboundMessage("dummyBody1", groupId = Some("someGroupId"), deduplicationId = None)
    val messageWithDeduplicationId = new InboundMessage("dummyBody2", groupId = None, deduplicationId = Some("123"))
    Sqs.fromConfig.use { case Sqs(_, producer, operator) =>
      for {
        queueUrl <- operator.createQueue(fifoQueueName)
        isInvalidGroupId <- producer.sendSingleMessage(messageWithGroupId, queueUrl).as(false)
          .onErrorHandle(ex => if(ex.getMessage.contains("MessageGroupId is invalid")) true else false)
        isInvalidDeduplicationId <- producer.sendSingleMessage(messageWithDeduplicationId, queueUrl)
          .onErrorHandle(ex => if(ex.getMessage.contains("MessageDeduplicationId is invalid")) true else false)
      } yield {
        isInvalidGroupId shouldBe true
        isInvalidDeduplicationId shouldBe true
      }
    }.runSyncUnsafe()
  }

  it should "respect delayDuration of messages " in {

    val delayedMessage =  genStandardMessage.map(_.copy(delayDuration = Some(5.seconds))).sample.get
    Sqs.fromConfig.use { case Sqs(receiver, producer, operator) =>
      for {
        queueUrl <- operator.createQueue(fifoQueueName)
        _ <- producer.sendSingleMessage(delayedMessage, queueUrl)
        receivedMessages1 <- Task.sleep(2.seconds) *> receiver.receiveSingleAutoDelete(queueUrl)
        receivedMessages2 <- Task.sleep(5.seconds) *> receiver.receiveSingleAutoDelete(queueUrl)
      } yield {
        receivedMessages1 shouldBe List.empty
        receivedMessages2.size shouldBe 1
        receivedMessages2.head.body shouldBe delayedMessage.body
      }
    }.runSyncUnsafe()
  }

  "A stream of received messages" can "be atomically consumed with automatic deletes" in {

    val n = 10
    val messages = Gen.listOfN(n, genStandardMessage).sample.get
    Sqs.fromConfig.use { case Sqs(receiver, producer, operator) =>
      for {
        queueUrl <- operator.createQueue(fifoQueueName)
        _ <- Observable.fromIterable(messages)
          .consumeWith(producer.sendSink(queueUrl))
        receivedMessages <- receiver.receiveAutoDelete(queueUrl)
          .take(n).toListL
      } yield {
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it can "be consumed with manual deletes" in {

    val n = 10
    val messages = Gen.listOfN(n, genStandardMessage).sample.get
    Sqs.fromConfig.use { case Sqs(receiver, producer, operator) =>
      for {
        queueUrl <- operator.createQueue(fifoQueueName)
        _ <- Observable.fromIterable(messages)
          .consumeWith(producer.sendSink(queueUrl))
        receivedMessages <- receiver.receiveManualDelete(queueUrl)
          .mapEvalF(deletable => deletable.deleteFromQueue().as(deletable))
          .take(n).toListL
      } yield {
        receivedMessages.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

}
