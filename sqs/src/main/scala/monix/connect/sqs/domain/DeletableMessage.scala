package monix.connect.sqs.domain

import monix.connect.sqs.SqsOp
import monix.eval.Task
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{DeleteMessageRequest, Message}

/**
  * Represents a message consumed with manual delete, meaning that it provides
  * control over when the message is considered processed and wants
  * to be deleted from the source queue so that the next message can be consumed.
  *
  * And that functionality is exposed by the [[deleteFromQueue]] method.
  */
class DeletableMessage private[sqs](override val queueUrl: QueueUrl,
                                    override protected val message: Message)
                                   (implicit asyncClient: SqsAsyncClient) extends ReceivedMessage(queueUrl, message) {

  def deleteFromQueue(): Task[Unit] = {
    val deleteMessageRequest = DeleteMessageRequest.builder
      .queueUrl(queueUrl.url)
      .receiptHandle(message.receiptHandle)
      .build
    SqsOp.deleteMessage.execute(deleteMessageRequest)(asyncClient).void
  }

}


