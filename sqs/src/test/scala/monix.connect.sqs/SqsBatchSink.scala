package monix.connect.sqs

import monix.connect.sqs.domain.QueueUrl
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class SqsBatchSink extends AnyFlatSpecLike with Matchers with SqsFixture {


  "A single inbound message" can "be send as if it was a batch" in {
    val message = genInboundMessage.sample.get
    val batches = SqsParBatchSink.groupMessagesInBatches(List(message), QueueUrl(""))
    batches.size shouldBe 1
    batches.flatten(_.entries().asScala).size shouldBe 1
  }

  "Ten inbound messages" must "be grouped in a single batch" in {
    val messages = Gen.listOfN(10, genInboundMessage).sample.get
    val batches = SqsParBatchSink.groupMessagesInBatches(messages, QueueUrl(""))
    batches.size shouldBe 1
    batches.flatten(_.entries().asScala).size shouldBe 10
  }

  "More than ten messages" must "be grouped in a single batch" in {
    val messages = Gen.listOfN(11, genInboundMessage).sample.get
    val batches = SqsParBatchSink.groupMessagesInBatches(messages, QueueUrl(""))
    batches.size shouldBe 2
    batches.flatten(_.entries().asScala).size shouldBe 11
  }

  "N messages" must "be grouped in a single batch" in {
    val n = Gen.choose(21, 1000).sample.get
    val messages = Gen.listOfN(n, genInboundMessage).sample.get
    val batches = SqsParBatchSink.groupMessagesInBatches(messages, QueueUrl(""))
    batches.size shouldBe (n / 10) + 1
    batches.flatten(_.entries().asScala).size shouldBe messages.size
  }

}
