package monix.connect.sqs

import org.scalacheck.Gen
import org.scalatest.TestSuite
import software.amazon.awssdk.services.sqs.model._

import scala.collection.JavaConverters._

trait SqsFixture {
  this: TestSuite =>

  val genQueueUrl: Gen[String] =
    Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "test-" + chars.mkString.take(200)).sample.get

  val genQueueName: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "test-" + chars.mkString.take(200))

  val genNamePrefix: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "test-" + chars.mkString.take(20))

  val genMessageId: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "msg-" + chars.mkString.take(5))

  val genReceiptHandle: Gen[String] =
    Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "rHandle-" + chars.mkString.take(10))

  val genMessageBody: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "body-" + chars.mkString.take(200))

  val message: Gen[Message] = for {
    id      <- genMessageId
    rhandle <- genReceiptHandle
    body    <- genMessageBody
  } yield Message.builder.messageId(id).receiptHandle(rhandle).body(body).build()

  def createQueueRequest(queueName: String): CreateQueueRequest =
    CreateQueueRequest
      .builder()
      .queueName(queueName)
      .attributes(
        Map(QueueAttributeName.DELAY_SECONDS -> "0", QueueAttributeName.MESSAGE_RETENTION_PERIOD -> "86400").asJava)
      .build()

  def listQueuesRequest(name_prefix: String): ListQueuesRequest =
    ListQueuesRequest.builder().queueNamePrefix(name_prefix).build()

  def getQueueUrlRequest(queueName: String): GetQueueUrlRequest =
    GetQueueUrlRequest.builder().queueName(queueName).build()

  def deleteQueueRequest(queueUrl: String): DeleteQueueRequest =
    DeleteQueueRequest.builder().queueUrl(queueUrl).build()

  def sendMessageRequest(queueUrl: String, messageBody: String): SendMessageRequest =
    SendMessageRequest
      .builder()
      .queueUrl(queueUrl)
      .messageBody(messageBody)

      .delaySeconds(0)
      .build()

  def receiveMessageRequest(queueUrl: String): ReceiveMessageRequest =
    ReceiveMessageRequest.builder().queueUrl(queueUrl).build()

  def deleteMessageRequest(queueUrl: String, message: Message): DeleteMessageRequest =
    DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(message.receiptHandle()).build()

  protected def queueUrl(queueName: String) = "http://localhost:4576/queue/" + queueName
}
