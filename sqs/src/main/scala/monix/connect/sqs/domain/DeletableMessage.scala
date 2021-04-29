package monix.connect.sqs.domain

import monix.connect.sqs.SqsOp
import monix.eval.Task
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{DeleteMessageRequest, Message}

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


