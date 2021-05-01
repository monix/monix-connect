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
import scala.jdk.CollectionConverters._

class ProducerSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get


  "parBatch" must "send a group of 10 message in the same batch" in {
    val groupId = "groupId"
    val queueName = genFifoQueueName.sample.get
    val messages = Gen.listOfN(10, genInboundMessageWithDeduplication).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- sqs.producer.parBatch(messages, queueUrl, Some(groupId))
      } yield {
        val batchEntryResponses = response.flatten(_.successful().asScala)
        response.exists(_.hasFailed()) shouldBe false
        batchEntryResponses.size shouldBe 10
        batchEntryResponses.map(_.md5OfMessageBody()) shouldBe messages.map(msg => md5Hex(msg.body))
      }
    }.runSyncUnsafe()
  }

  it must "allow passing a group of 10 messages by splitting requests in parallel batches of at most 10 entries" in {
    val groupId = "groupId"
    val queueName = genFifoQueueName.sample.get
    val numOfEntries = Gen.choose(11, 100).sample.get
    val messages = Gen.listOfN(numOfEntries, genInboundMessageWithDeduplication).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- sqs.producer.parBatch(messages, queueUrl, Some(groupId))
      } yield {
        response.size shouldBe (numOfEntries / 10) + 1
        response.exists(_.hasFailed()) shouldBe false
        val successfulResponses = response.flatten(_.successful().asScala)
        successfulResponses.size shouldBe numOfEntries
        successfulResponses.map(_.md5OfMessageBody()) shouldBe messages.map(msg => md5Hex(msg.body))
      }
    }.runSyncUnsafe()
  }

  "parBatchSink" should "send in a single batch, a group of less than 10 messages emitted at once" in {
    val groupId = "groupId"
    val queueName = genFifoQueueName.sample.get
    val messages = Gen.listOfN(5, genInboundMessageWithDeduplication).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.now(messages).consumeWith(sqs.producer.parBatchSink(queueUrl, Some(groupId)))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(2.seconds).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe messages.map(_.body)
      }
    }.runSyncUnsafe()
  }

  it should "do nothing when consuming an empty observable" in {
    val groupId = "groupId"
    val queueName = genFifoQueueName.sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.empty[List[InboundMessage]].consumeWith(sqs.producer.parBatchSink(queueUrl, Some(groupId)))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(2.seconds).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) shouldBe List.empty
      }
    }.runSyncUnsafe()
  }

  it should "send in batches of at most 10 messages, a group of `N` emitted at once" in {
    val groupId = "groupId"
    val queueName = genFifoQueueName.sample.get
    val messages =  Gen.choose(11, 100).flatMap(Gen.listOfN(_, genInboundMessageWithDeduplication)).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        response <- Observable.now(messages).consumeWith(sqs.producer.parBatchSink(queueUrl, Some(groupId)))
        result <- sqs.consumer.receiveAutoDelete(queueUrl).bufferTimed(2.seconds).firstL
      } yield {
        response shouldBe a [Unit]
        result.map(_.body) should contain theSameElementsAs messages.map(_.body)
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
