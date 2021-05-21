package monix.connect.sqs

import monix.connect.sqs.domain.QueueName
import monix.connect.sqs.domain.inbound.InboundMessage
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
import scala.jdk.CollectionConverters._

class ProducerSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get

  "parSendMessage" must "send a group of 10 message in the same batch" in {
    val queueName = genFifoQueueName.sample.get
    val messages = Gen.listOfN(10, genFifoMessage(defaultGroupId)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = fifoDeduplicationQueueAttr)
        response <- sqs.producer.parSendBatch(messages, queueUrl)
      } yield {
        val batchEntryResponses = response.flatten(_.successful().asScala)
        response.exists(_.hasFailed()) shouldBe false
        batchEntryResponses.size shouldBe 10
        batchEntryResponses.map(_.md5OfMessageBody()) shouldBe messages.map(msg => md5Hex(msg.body))
      }
    }.runSyncUnsafe()
  }

  it must "allow passing a group of 10 messages by splitting requests in parallel batches of at most 10 entries" in {
    val queueName = genFifoQueueName.sample.get
    val numOfEntries = Gen.choose(11, 100).sample.get
    val messages = Gen.listOfN(numOfEntries, genFifoMessage(defaultGroupId)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = fifoDeduplicationQueueAttr)
        response <- sqs.producer.parSendBatch(messages, queueUrl)
      } yield {
        response.size shouldBe (numOfEntries / 10) + 1
        response.exists(_.hasFailed()) shouldBe false
        val successfulResponses = response.flatten(_.successful().asScala)
        successfulResponses.size shouldBe numOfEntries
        successfulResponses.map(_.md5OfMessageBody()) shouldBe messages.map(msg => md5Hex(msg.body))
      }
    }.runSyncUnsafe()
  }

  "parSink" should "sends in a single batch, a group of less than 10 messages emitted at once" in {
    val queueName = genFifoQueueName.sample.get
    val messages = Gen.listOfN(5, genFifoMessage(defaultGroupId)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.now(messages).consumeWith(sqs.producer.parBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(2.seconds).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it should "do nothing and gracefully terminate on completion, when consuming an empty observable" in {
    val queueName = genFifoQueueName.sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.empty[List[InboundMessage]].consumeWith(sqs.producer.parBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  it should "send in batches of at most 10 messages, a group of `N` emitted at once" in {
    val queueName = genFifoQueueName.sample.get
    val messages =  Gen.choose(11, 100).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.now(messages).consumeWith(sqs.producer.parBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  "sink" should "send each emitted message individually" in {
    val queueName = genFifoQueueName.sample.get

    val messages = Gen.choose(1, 100).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.fromIterable(messages).consumeWith(sqs.producer.sink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it should "do nothing and gracefully terminate `onCompletion` when consuming an empty observable" in {
    val queueName = genFifoQueueName.sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.empty[InboundMessage].consumeWith(sqs.producer.sink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe List.empty
      }
    }.runSyncUnsafe()
  }



}
