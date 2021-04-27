package monix.connect.sqs.domain

import monix.connect.sqs.Sqs
import monix.eval.Task
import software.amazon.awssdk.services.sqs.model.Message

class ReceivedMessage(sqs: Sqs, val queueUrl: QueueUrl, val message: Message) {

  def deleteFromQueue(): Task[Unit] = {
    sqs.deleteMessage(queueUrl, message.receiptHandle)
  }

}
