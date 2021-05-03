package monix.connect.sqs

import monix.connect.sqs.SqsParBatchSink.groupMessagesInBatches
import monix.connect.sqs.domain.{InboundMessage, QueueUrl}
import monix.eval.Task
import monix.reactive.Consumer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SendMessageBatchResponse, SendMessageResponse}

import scala.concurrent.duration.FiniteDuration

object SqsProducer {
  def create(implicit asyncClient: SqsAsyncClient): SqsProducer = new SqsProducer(asyncClient)
}

class SqsProducer private[sqs](asyncClient: SqsAsyncClient) {

  def sendSingleMessage(message: InboundMessage,
                        queueUrl: QueueUrl,
                        delayDuration: Option[FiniteDuration] = None): Task[SendMessageResponse] = {
    val producerMessage = message.toMessageRequest(queueUrl, delayDuration)
    SqsOp.sendMessage.execute(producerMessage)(asyncClient)
  }

  def parSendMessages(messages: List[InboundMessage],
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
           stopOnError: Boolean = false): Consumer[InboundMessage, Unit] = {
    val toJavaMessage = (message: InboundMessage) => message.toMessageRequest(queueUrl, delayDuration)
    new SqsSink(toJavaMessage, SqsOp.sendMessage, asyncClient, stopOnError)
  }

  def parSink(queueUrl: QueueUrl,
              delayDuration: Option[FiniteDuration] = None,
              stopOnError: Boolean = false): Consumer[List[InboundMessage], Unit] = {
    new SqsParBatchSink(queueUrl, delayDuration, asyncClient, stopOnError)
  }
}
