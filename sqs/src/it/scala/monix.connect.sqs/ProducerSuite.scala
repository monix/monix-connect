package monix.connect.sqs

import monix.connect.sqs.inbound.InboundMessage
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.QueueAttributeName

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ProducerSuite extends AnyFlatSpecLike with Matchers with BeforeAndAfterEach with SqsITFixture {

  "parSendMessage" must "send a group of 10 message in the same batch" in {
    val messages = Gen.listOfN(10, genFifoMessage(defaultGroupId)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- sqs.producer.sendParBatch(messages, queueUrl)
      } yield {
        val batchEntryResponses = response.flatten(_.successful().asScala)
        response.exists(_.hasFailed()) shouldBe false
        batchEntryResponses.size shouldBe 10
        batchEntryResponses.map(_.md5OfMessageBody()) shouldBe messages.map(msg => md5Hex(msg.body))
      }
    }.runSyncUnsafe()
  }

  it must "allow passing a group of 10 messages by splitting requests in parallel batches of at most 10 entries" in {
    val numOfEntries = Gen.choose(11, 199).sample.get
    val messages = Gen.listOfN(numOfEntries, genFifoMessage(defaultGroupId)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        responses <- sqs.producer.sendParBatch(messages, queueUrl)
      } yield {
        responses.size shouldBe (numOfEntries / 10) + (if((numOfEntries % 10)==0) 0 else 1)
        responses.exists(_.hasFailed()) shouldBe false
        val successfulResponses = responses.flatten(_.successful().asScala)
        successfulResponses.size shouldBe numOfEntries
        successfulResponses.map(_.md5OfMessageBody()) shouldBe messages.map(msg => md5Hex(msg.body))

      }
    }.runSyncUnsafe()
  }

  it must "not fail when on empty messages list" in {
    val messages = List.empty[InboundMessage]
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- sqs.producer.sendParBatch(messages, queueUrl)
      } yield {
        response.size shouldBe 0
        messages.size shouldBe 0
      }
    }.runSyncUnsafe()
  }

  "parSink" should "sends in a single batch, a group of less than 10 messages emitted at once" in {
    val messages = Gen.listOfN(5, genFifoMessage(defaultGroupId)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.now(messages).consumeWith(sqs.producer.sendParBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(2.seconds).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it should "do nothing when consuming an empty observable, and gracefully terminate on completion" in {

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.empty[List[InboundMessage]].consumeWith(sqs.producer.sendParBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  it should "send in batches of at most 10 messages, a group of `N` emitted at once" in {
    val messages =  Gen.choose(11, 100).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.now(messages).consumeWith(sqs.producer.sendParBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  "sink" should "send each emitted message individually" in {

    val messages = Gen.choose(1, 100).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.fromIterable(messages).consumeWith(sqs.producer.sendSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(3.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it should "do nothing when consuming an empty observable, and gracefully terminate `onCompletion` " in {

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.empty[InboundMessage].consumeWith(sqs.producer.sendSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

}
