package monix.connect.sqs

import monix.connect.sqs.producer.Message
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskTest
import org.apache.commons.codec.digest.DigestUtils.md5Hex
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.QueueAttributeName

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ProducerSuite extends AsyncFlatSpec with MonixTaskTest with Matchers with BeforeAndAfterAll with SqsITFixture {

  override implicit val scheduler = Scheduler.io("sqs-producer-suite")

  "parSendMessage" must "send a group of 10 message in the same batch" in {
    val messages = Gen.listOfN(10, genFifoMessage(defaultGroupId)).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- sqs.producer.sendParBatch(messages, queueUrl)
      } yield {
        val batchEntryResponses = response.flatten(_.successful().asScala)
        response.exists(_.hasFailed()) shouldBe false
        batchEntryResponses.size shouldBe 10
        batchEntryResponses.map(_.md5OfMessageBody()) shouldBe messages.map(msg => md5Hex(msg.body))
      }
  }

  it must "allow passing a group of `n` messages by splitting requests in parallel batches of at most 10 entries" in {
    val numOfEntries = Gen.choose(11, 199).sample.get
    val messages = Gen.listOfN(numOfEntries, genFifoMessage(defaultGroupId)).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        responses <- sqs.producer.sendParBatch(messages, queueUrl)
      } yield {
        responses.size shouldBe (numOfEntries / 10) + (if((numOfEntries % 10)==0) 0 else 1)
        responses.exists(_.hasFailed()) shouldBe false
        val successfulResponses = responses.flatten(_.successful().asScala)
        successfulResponses.size shouldBe numOfEntries
        successfulResponses.map(_.md5OfMessageBody()) shouldBe messages.map(msg => md5Hex(msg.body))

      }
  }

  it must "not fail on empty messages list" in {
    val messages = List.empty[Message]
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- sqs.producer.sendParBatch(messages, queueUrl)
      } yield {
        response.size shouldBe 0
        messages.size shouldBe 0
      }
  }

  "sendParBatchSink" should "send in a single batch, a group of less than 10 messages emitted at once" in {
    val messages = Gen.listOfN(5, genFifoMessage(defaultGroupId)).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.now(messages).consumeWith(sqs.producer.sendParBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(2.seconds).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe messages.map(_.body)
      }
  }

  it should "do nothing when consuming an empty observable, and gracefully terminate on completion" in {
     for {
       sqs <- unsafeSqsAsyncClient
       fifoQueueName <- Task.from(genFifoQueueName)
       queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.empty[List[Message]].consumeWith(sqs.producer.sendParBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe List.empty
      }
  }

  it should "send in batches of `n` messages" in {
    val messages =  Gen.choose(11, 21).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get
     for {
       sqs <- unsafeSqsAsyncClient
       fifoQueueName <- Task.from(genFifoQueueName)
       queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.now(messages).consumeWith(sqs.producer.sendParBatchSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(4.seconds).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) should contain theSameElementsAs messages.map(_.body)
      }
  }

  "sink" should "send each emitted message individually" in {

    val messages = Gen.choose(1, 100).flatMap(Gen.listOfN(_, genFifoMessage(defaultGroupId))).sample.get
      for {
        sqs <- unsafeSqsAsyncClient
        fifoQueueName <- Task.from(genFifoQueueName)
        queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = fifoDeduplicationQueueAttr)
        response <- Observable.fromIterable(messages).consumeWith(sqs.producer.sendSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(5.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe messages.map(_.body)
      }
  }

  it should "do nothing when consuming an empty observable, and gracefully terminate `onCompletion` " in {

    for {
      sqs <- unsafeSqsAsyncClient
      fifoQueueName <- Task.from(genFifoQueueName)
      queueUrl <- sqs.operator.createQueue(fifoQueueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.empty[Message].consumeWith(sqs.producer.sendSink(queueUrl))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(1.second).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe List.empty
      }
  }

}
