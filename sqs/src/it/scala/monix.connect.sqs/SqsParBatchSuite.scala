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

class SqsParBatchSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get


  "A list of messages of size 10" should "be sent in the same batch" in {
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

  "A list bigger than 10 messages" must "be splitted and sent in batches of at most 10 entries" in {
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
