package monix.connect.sqs

import monix.connect.sqs.domain.inbound.{FifoMessage, StandardMessage}
import monix.connect.sqs.domain.{QueueName, QueueUrl}
import org.scalacheck.Gen
import org.scalatest.TestSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import java.net.URI
import scala.collection.JavaConverters._

trait SqsFixture {
  this: TestSuite =>

  val nonExistingQueueErrorMsg: String =
    """Invalid request: MissingQueryParamRejection(QueueName), MissingFormFieldRejection(QueueUrl); see the SQS docs. (Service: Sqs, Status Code: 400, Request ID: 00000000-0000-0000-0000-000000000000, Extended Request ID: null)""".stripMargin

  val defaultAwsCredProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
  val asyncClient =
    SqsAsyncClient
      .builder
      .credentialsProvider(defaultAwsCredProvider)
      .endpointOverride(new URI("http://localhost:9324"))
      .region(Region.US_EAST_1)
      .build

  val fifoDeduplicationQueueAttr = Map(
    QueueAttributeName.FIFO_QUEUE -> "true",
    QueueAttributeName.CONTENT_BASED_DEDUPLICATION -> "true")


  def queueUrlPrefix(queueName: String) = s"http://localhost:9324/000000000000/${queueName}"

  val genQueueName: Gen[QueueName] = Gen.identifier.map(id => QueueName("queue-" + id.take(30)))

  // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
  val genFifoQueueName: Gen[QueueName] = Gen.identifier.map(id => QueueName("queue-" + id.take(20) + ".fifo"))
  val genGroupId: Gen[String] = Gen.identifier.map(id => "groupId-" + id.take(10))
  val genId: Gen[String] = Gen.identifier.map(_.take(15))
  val defaultGroupId: String = genGroupId.sample.get
  val genNamePrefix: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "test-" + chars.mkString.take(20))
  val genMessageId: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "msg-" + chars.mkString.take(5))
  val genReceiptHandle: Gen[String] =
    Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "rHandle-" + chars.mkString.take(10))
  val genMessageBody: Gen[String] = Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "body-" + chars.mkString.take(200))
  def genFifoMessageWithDeduplication(groupId: String = defaultGroupId): Gen[FifoMessage] =
    Gen.identifier.map(_.take(10)).map(id => FifoMessage(id, groupId = groupId, deduplicationId = Some(id)))

  def genFifoMessage(groupId: String = defaultGroupId, deduplicationId: Option[String] = None): Gen[FifoMessage] = Gen.identifier.map(_.take(10)).map(id => FifoMessage(id, groupId = groupId, deduplicationId = deduplicationId))
  val genQueueUrl: Gen[QueueUrl] = QueueUrl(genId.sample.get)

  val genStandardMessage: Gen[StandardMessage] = Gen.identifier.map(_.take(10)).map(StandardMessage(_))

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
        Map(QueueAttributeName.DELAY_SECONDS -> "60", QueueAttributeName.MESSAGE_RETENTION_PERIOD -> "86400").asJava)
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

}
