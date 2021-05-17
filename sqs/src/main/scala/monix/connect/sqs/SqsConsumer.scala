package monix.connect.sqs

import monix.connect.sqs.domain.{DeletableMessage, QueueUrl, ReceivedMessage}
import monix.eval.Task
import monix.reactive.Observable
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{QueueAttributeName, ReceiveMessageRequest}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

private[sqs] object SqsConsumer {
  def create(implicit asyncClient: SqsAsyncClient): SqsConsumer = new SqsConsumer()
}

class SqsConsumer private[sqs](implicit asyncClient: SqsAsyncClient) {

  /**
    * Starts the process of consuming deletable messages
    * from the specified `queueUrl`.
    *
    * A [[DeletableMessage]] provides the control over when the message is considered as
    * processed, thus can be deleted from the source queue and allow the next message
    * to be consumed.
    *
    * @see [[receiveManualDelete]] for at least once semantics.
    *
    * @param queueUrl source queue url, obtained from [[SqsOperator.getQueueUrl]].
    * @param waitTimeSeconds The duration (in seconds) for which the call waits for a message
    *                        to arrive in the queue before returning. If a message is available,
    *                        the call returns sooner than WaitTimeSeconds.
    *                        If no messages are available and the wait time expires,
    *                        the call returns successfully with an empty list of messages.
    *
    */
  def receiveAutoDelete(queueUrl: QueueUrl,
                        waitTimeSeconds: FiniteDuration = Duration.Zero,
                        autoDeleteRequestRetry: Int = 3): Observable[ReceivedMessage] = {
    receiveManualDelete(queueUrl,
      // 10 as per the maximum configurable and would give the highest performance,
      // though the user would not notice about it.
      inFlightMessages = 10,
      // visibility timeout does not apply to already deleted messages
      visibilityTimeout = 30.seconds,
      waitTimeSeconds = waitTimeSeconds)
      .mapEvalF { deletableMessage =>
        deletableMessage.deleteFromQueue()
          .as(deletableMessage)
          .onErrorRestart(autoDeleteRequestRetry)
      }
  }

  /**
    * Starts the process of consuming deletable messages
    * from the specified `queueUrl`.
    *
    * A [[DeletableMessage]] provides the control over when the message is considered as
    * processed, thus can be deleted from the source queue and allow the next message
    * to be consumed.
    *
    * @see [[receiveAutoDelete]] for at most once semantics.
    *
    * @param queueUrl source queue url, obtained from [[SqsOperator.getQueueUrl]].
    * @param inFlightMessages max number of message to be consumed at a time, meaning that
    *                         we would not be able to consume further message from the same
    *                         queue and groupId until at least one of the previous batch
    *                         was deleted from the queue.
    * @param visibilityTimeout The duration (in seconds) that the received messages are hidden
    *                          from subsequent retrieve requests after being retrieved (in case they
    *                          have not been deleted before).
    * @param waitTimeSeconds The duration (in seconds) for which the call waits for a message
    *                        to arrive in the queue before returning. If a message is available,
    *                        the call returns sooner than WaitTimeSeconds.
    *                        If no messages are available and the wait time expires,
    *                        the call returns successfully with an empty list of messages.
    *
    */
  def receiveManualDelete(queueUrl: QueueUrl,
                          inFlightMessages: Int = 10,
                          visibilityTimeout: FiniteDuration = 30.seconds,
                          waitTimeSeconds: FiniteDuration = Duration.Zero): Observable[DeletableMessage] = {
    Observable.repeatEvalF {
      receiveSingleManualDelete(queueUrl,
        inFlightMessages = inFlightMessages,
        visibilityTimeout = visibilityTimeout,
        waitTimeSeconds = waitTimeSeconds)
    }.flatMap { deletableMessages =>
      Observable.fromIterable(deletableMessages)
    }
  }

  /**
    * Single receive message action that responds with a list of
    * **already** deleted messages from the specified `queueUrl`.
    *
    * Meaning that the semantics that this method provides are **at most once**,
    * since as the message is automatically deleted when consumed, if there is
    * a failure during the processing of the message that would be lost as it
    * could not be read again from the queue.
    *
    * @see [[receiveSingleManualDelete]] for at least once semantics.
    *
    * A [[DeletableMessage]] provides the control over when the message is considered as
    * processed, thus can be deleted from the source queue and allow the next message
    * to be consumed.
    *
    * @param queueUrl source queue url, obtained from [[SqsOperator.getQueueUrl]].
    * @param inFlightMessages max number of message to be consumed at a time, meaning that
    *                         we would not be able to consume further message from the same
    *                         queue and groupId until at least one of the previous batch
    *                         was deleted from the queue.
    *                         **Important** the request will fail for numbers higher than 10.
    * @param waitTimeSeconds The duration (in seconds) for which the call waits for a message to arrive
    *                        in the queue before returning. If a message is available, the call returns
    *                        sooner than WaitTimeSeconds. If no messages are available and the wait time expires,
    *                        the call returns successfully with an empty list of messages.
    * @return a list of [[DeletableMessage]]s of size of at most as `inFlightMessages` and the maximum available
    *         in the queue at that time.
    */
  def receiveSingleAutoDelete(queueUrl: QueueUrl,
                              inFlightMessages: Int = 10,
                              waitTimeSeconds: FiniteDuration = Duration.Zero): Task[List[DeletableMessage]] = {
    val receiveRequest = singleReceiveRequest(queueUrl,
      maxMessages = inFlightMessages,
      visibilityTimeout = 30.seconds,
      waitTimeSeconds = waitTimeSeconds)
    Task.evalAsync(receiveRequest).flatMap {
      SqsOp.receiveMessage.execute(_)
        .map(_.messages.asScala.toList
          .map(msg => new DeletableMessage(queueUrl, msg)))
    }
  }

  /**
    * Single receive message action that responds with a list of
    * deletable messages from the specified `queueUrl`.
    *
    * A [[DeletableMessage]] provides the control over when the message is considered as
    * processed, thus can be deleted from the source queue and allow the next message
    * to be consumed.
    *
    * @see [[receiveSingleAutoDelete]] for at most once semantics.
    *
    * @param queueUrl source queue url, obtained from [[SqsOperator.getQueueUrl]].
    * @param inFlightMessages max number of message to be consumed at a time, at most 10!
    *                         meaning that we would not be able to consume further message
    *                         from the same queue and groupId until at least one of the
    *                         previous batch was deleted from the queue.
    *                         **Important** the request will fail for numbers higher than 10.
    * @param visibilityTimeout The duration (in seconds) that the received messages are hidden
    *                          from subsequent retrieve requests after being retrieved (in case they
    *                          have not been deleted before).
    * @param waitTimeSeconds The duration (in seconds) for which the call waits for a message to arrive
    *                        in the queue before returning. If a message is available, the call returns
    *                        sooner than WaitTimeSeconds. If no messages are available and the wait time expires,
    *                        the call returns successfully with an empty list of messages.
    * @return a list of [[DeletableMessage]]s of size of at most as `inFlightMessages` and the maximum available
    *         in the queue at that time.
    */
  def receiveSingleManualDelete(queueUrl: QueueUrl,
                                inFlightMessages: Int = 10,
                                visibilityTimeout: FiniteDuration = 30.seconds,
                                waitTimeSeconds: FiniteDuration = Duration.Zero): Task[List[DeletableMessage]] = {
    val receiveRequest = singleReceiveRequest(queueUrl,
      maxMessages = inFlightMessages,
      visibilityTimeout = visibilityTimeout,
      waitTimeSeconds = waitTimeSeconds)
    Task.evalAsync(receiveRequest).flatMap {
      SqsOp.receiveMessage.execute(_)
        .map(_.messages.asScala.toList
          .map(msg => new DeletableMessage(queueUrl, msg)))
    }
  }

  private[this] def singleReceiveRequest(queueUrl: QueueUrl,
                                         maxMessages: Int,
                                         visibilityTimeout: FiniteDuration,
                                         waitTimeSeconds: FiniteDuration = Duration.Zero): ReceiveMessageRequest = {
    val builder = ReceiveMessageRequest.builder
      .queueUrl(queueUrl.url)
      .maxNumberOfMessages(maxMessages)
      .attributeNames(QueueAttributeName.ALL)
      .visibilityTimeout(visibilityTimeout.toSeconds.toInt)
    if (waitTimeSeconds.toSeconds > 0L) builder.waitTimeSeconds(waitTimeSeconds.toSeconds.toInt)
    builder.build
  }

}
