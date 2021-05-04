package monix.connect.sqs

import monix.connect.sqs.SqsParBatchSink.groupMessagesInBatches
import monix.connect.sqs.domain.{InboundMessage, QueueUrl}
import monix.eval.Task
import monix.reactive.Consumer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SendMessageBatchResponse, SendMessageResponse}

import scala.concurrent.duration.FiniteDuration

private[sqs] object SqsProducer {
  def create(implicit asyncClient: SqsAsyncClient): SqsProducer = new SqsProducer(asyncClient)
}

class SqsProducer private[sqs](asyncClient: SqsAsyncClient) {

  def sendSingleMessage(message: InboundMessage,
                        queueUrl: QueueUrl,
                        delayDuration: Option[FiniteDuration] = None): Task[SendMessageResponse] = {
    val producerMessage = message.toMessageRequest(queueUrl, delayDuration)
    SqsOp.sendMessage.execute(producerMessage)(asyncClient)
  }

  def parSendBatch(messages: List[InboundMessage],
                   queueUrl: QueueUrl,
                   delayDuration: Option[FiniteDuration] = None): Task[List[SendMessageBatchResponse]] = {
    Task.parTraverse {
      groupMessagesInBatches(messages, queueUrl, delayDuration)
    } { batch =>
      SqsOp.sendMessageBatch.execute(batch)(asyncClient)
    }
  }

  def sink(queueUrl: QueueUrl,
           delayDuration: Option[FiniteDuration] = None,
           stopOnError: Boolean = false): Consumer[InboundMessage, Unit] =
    SqsSink.send(queueUrl, delayDuration, SqsOp.sendMessage, asyncClient, stopOnError)


  def parBatchSink(queueUrl: QueueUrl,
                   delayDuration: Option[FiniteDuration] = None,
                   stopOnError: Boolean = false): Consumer[List[InboundMessage], Unit] =
    new SqsParBatchSink(queueUrl, delayDuration, asyncClient, stopOnError)

}
