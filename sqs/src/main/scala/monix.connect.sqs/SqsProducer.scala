package monix.connect.sqs

import monix.connect.sqs.SqsParBatchSink.groupMessagesInBatches
import monix.connect.sqs.domain.{InboundMessage, QueueUrl}
import monix.eval.Task
import monix.reactive.Consumer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SendMessageBatchRequest, SendMessageBatchResponse, SendMessageResponse, SqsRequest, SqsResponse}

import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

object SqsProducer {
  def create(implicit asyncClient: SqsAsyncClient): SqsProducer = new SqsProducer(asyncClient)
}

class SqsProducer private[sqs](asyncClient: SqsAsyncClient) {

  def sendSingleMessage(message: InboundMessage,
                        queueUrl: QueueUrl,
                        groupId: Option[String] = None,
                        delayDuration: Option[FiniteDuration] = None): Task[SendMessageResponse] = {
    val producerMessage = message.toMessageRequest(queueUrl, groupId, delayDuration)
    SqsOp.sendMessage.execute(producerMessage)(asyncClient)
  }

  def parBatch(messages: List[InboundMessage],
               queueUrl: QueueUrl,
               groupId: Option[String] = None,
               delayDuration: Option[FiniteDuration] = None): Task[List[SendMessageBatchResponse]] = {
    Task.parTraverse {
      groupMessagesInBatches(messages, queueUrl, groupId, delayDuration)
    } { batch =>
      SqsOp.sendMessageBatch.execute(batch)(asyncClient)
    }
  }

  def sink(queueUrl: QueueUrl,
           groupId: Option[String] = None,
           delayDuration: Option[FiniteDuration] = None,
           stopOnError: Boolean = false): Consumer[InboundMessage, Unit] = {
    val toJavaMessage = (message: InboundMessage) => message.toMessageRequest(queueUrl, groupId, delayDuration)
    new SqsSink(toJavaMessage, SqsOp.sendMessage, asyncClient, stopOnError)
  }

  def parBatchSink(queueUrl: QueueUrl,
                   groupId: Option[String] = None,
                   delayDuration: Option[FiniteDuration] = None,
                   stopOnError: Boolean = false): Consumer[List[InboundMessage], Unit] = {
    new SqsParBatchSink(queueUrl, groupId, delayDuration, asyncClient, stopOnError)
  }
}
