package monix.connect.sqs

import monix.connect.sqs.SqsParBatchSink.groupMessagesInBatches
import monix.connect.sqs.domain.QueueUrl
import monix.connect.sqs.domain.inbound.InboundMessage
import monix.eval.Task
import monix.reactive.Consumer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SendMessageBatchResponse, SendMessageResponse}

import scala.concurrent.duration.FiniteDuration

private[sqs] object SqsProducer {
  def create(implicit asyncClient: SqsAsyncClient): SqsProducer = new SqsProducer(asyncClient)
}

class SqsProducer private[sqs](asyncClient: SqsAsyncClient) {

  /** Sends a single message to the specified queue. */
  def sendSingleMessage(message: InboundMessage,
                        queueUrl: QueueUrl,
                        delayDuration: Option[FiniteDuration] = None): Task[SendMessageResponse] = {
    val producerMessage = message.toMessageRequest(queueUrl)
    SqsOp.sendMessage.execute(producerMessage)(asyncClient)
  }

  /**
    * Sends a list of `n` messages in parallel.
    *
    * By design the Sqs batch send request accepts at most 10 entries,
    * but the [[parSendBatch]] implementation overcomes that rule
    * by splitting long given lists of `messages` into batches of 10.
    * This is a great enhancement that makes the user not having to deal
    * nor worry about that limitation.
    *
    * @param messages the messages sent to the queue.
    * @param queueUrl target queue url
    * @return a list of [[SendMessageBatchResponse]] with the result of
    *         all the batch send requests performed.
    *         if the input list of `messages` is:
    *         - empty, it returns an empty list
    *         - smaller than 10, it returns a list of a single batch response
    *         - bigger than that it will return as much elements
    *           proportional to the module of 10.
    */
  def parSendBatch(messages: List[InboundMessage],
                   queueUrl: QueueUrl): Task[List[SendMessageBatchResponse]] = {
    if(messages.nonEmpty) {
      Task.parTraverse {
        groupMessagesInBatches(messages, queueUrl)
      } { batch =>
        SqsOp.sendMessageBatch.execute(batch)(asyncClient)
      }
    } else {
      Task.now(List.empty)
    }
  }

  /**
    *
    * @param queueUrl target queue url
    * @param stopOnError binary value indicating whether the subscriber should
    *                    stop if there is an error, or continue to producing incoming
    *                    messages.
    * @return a [[Consumer]] that expects [[InboundMessage]]s and never signals completion,
    *         it stops when whether the upstream signals `onComplete` or if it signals `onError`
    *         and `stopOnError` is set to `true`.
    *
    */
  def sink(queueUrl: QueueUrl,
           stopOnError: Boolean = false): Consumer[InboundMessage, Unit] =
    SqsSink.send(queueUrl, SqsOp.sendMessage, asyncClient, stopOnError)


  def parBatchSink(queueUrl: QueueUrl,
                   stopOnError: Boolean = false): Consumer[List[InboundMessage], Unit] =
    new SqsParBatchSink(queueUrl, asyncClient, stopOnError)

}
