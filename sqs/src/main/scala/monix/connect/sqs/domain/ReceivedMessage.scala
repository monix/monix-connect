package monix.connect.sqs.domain

import monix.connect.sqs.{SqsConsumer, SqsOp}
import monix.eval.Task
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityRequest, Message, MessageSystemAttributeName}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * Represents the generic implementation of a message consumed from a specific Sqs Queue.
  * This representation would typically be used when consuming messages in an auto delete way.
  * On the other hand, we would use manual deletes with [[DeletableMessage]].
  */
class ReceivedMessage private[sqs] (val queueUrl: QueueUrl, protected val message: Message)(implicit asyncClient: SqsAsyncClient) {

  val body: String = message.body()
  val messageId: String = message.messageId()
  val attributes: Map[MessageSystemAttributeName, String] = message.attributes().asScala.toMap
  val md5OfBody: String = message.md5OfBody()

  def changeVisibilityTimeout(timeout: FiniteDuration): Task[Unit] = {
    val changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder
      .queueUrl(queueUrl.url)
      .receiptHandle(message.receiptHandle)
      .visibilityTimeout(timeout.toSeconds.toInt).build
    SqsOp.changeMessageVisibility.execute(changeMessageVisibilityRequest)(asyncClient).void
  }

}

