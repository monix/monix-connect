package monix.connect.sqs

import monix.execution.Scheduler.Implicits.global
import monix.connect.sqs.producer.{FifoMessage, StandardMessage}
import monix.connect.sqs.domain.{QueueName, QueueUrl}
import monix.eval.{Task, TaskLike}
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterEach, TestSuite}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import java.net.URI
import scala.concurrent.Future

trait SqsITFixture {
  this: BeforeAndAfterEach =>

  override def beforeEach(): Unit = {
    val deleteAll = unsafeSqsAsyncClient.operator.listQueueUrls().mapEvalF(unsafeSqsAsyncClient.operator.deleteQueue).completedL.attempt
    deleteAll.runSyncUnsafe()
  }

  def dlqRedrivePolicyAttr(dlQueueArn: String) = Map(QueueAttributeName.REDRIVE_POLICY -> s"""{"maxReceiveCount":"1", "deadLetterTargetArn": "$dlQueueArn" }""")

  val invalidRequestErrorMsg: String = """Invalid request""".stripMargin

  val defaultAwsCredProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
  val asyncClient =
    SqsAsyncClient
      .builder
      .credentialsProvider(defaultAwsCredProvider)
      .endpointOverride(new URI("http://localhost:9324"))
      .region(Region.US_EAST_1)
      .build

  implicit val unsafeSqsAsyncClient: Sqs = Sqs.createUnsafe(asyncClient)

  val fifoDeduplicationQueueAttr = Map(
    QueueAttributeName.FIFO_QUEUE -> "true",
    QueueAttributeName.CONTENT_BASED_DEDUPLICATION -> "true")

  // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
  protected val genFifoQueueName: Gen[QueueName] = Gen.uuid.map(id => QueueName(s"queue-${id.toString}.fifo"))

  def queueUrlPrefix(queueName: String) = s"http://localhost:9324/000000000000/${queueName}"

  val queueName: QueueName = QueueName("queue-1")

  // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
  val genGroupId: Gen[String] = Gen.identifier.map(_.take(10))
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

  val genStandardMessage: Gen[StandardMessage] = Gen.identifier.map(_.take(12)).map(StandardMessage(_))

  val message: Gen[Message] = for {
    id      <- genMessageId
    rhandle <- genReceiptHandle
    body    <- genMessageBody
  } yield Message.builder.messageId(id).receiptHandle(rhandle).body(body).build()

  implicit val fromGen: TaskLike[Gen] =
    new TaskLike[Gen] {
      def apply[A](fa: Gen[A]): Task[A] =
        Task(fa.sample.get)
    }
}
