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
    *
    * @param queueUrl
    * @return
    */
  def receiveManualDelete(queueUrl: QueueUrl,
                          inFlightMessages: Int = 10,
                          visibilityTimeout: FiniteDuration = 30.seconds,
                          waitTimeSeconds: FiniteDuration = Duration.Zero): Observable[DeletableMessage] = {
    Observable.repeatEvalF {
      singleManualDelete(queueUrl,
        maxMessages = inFlightMessages,
        visibilityTimeout = visibilityTimeout,
        waitTimeSeconds = waitTimeSeconds)
    }.flatMap { deletableMessages =>
      Observable.fromIterable(deletableMessages)
    }
  }

  /**
    *
    * @param queueUrl
    * @return
    */
  def singleManualDelete(queueUrl: QueueUrl,
                         maxMessages: Int = 10,
                         visibilityTimeout: FiniteDuration = 30.seconds,
                         waitTimeSeconds: FiniteDuration = Duration.Zero): Task[List[DeletableMessage]] = {
    val receiveRequest = singleReceiveRequest(queueUrl,
      maxMessages = maxMessages,
      visibilityTimeout = visibilityTimeout,
      waitTimeSeconds = waitTimeSeconds)
    Task.evalAsync(receiveRequest).flatMap {
      SqsOp.receiveMessage.execute(_)
        .map(_.messages.asScala.toList
          .map(msg => new DeletableMessage(queueUrl, msg)))
    }
  }

  /**
    *
    * @param queueUrl
    * @return
    */
  def singleAutoDelete(queueUrl: QueueUrl,
                         maxMessages: Int = 10,
                         visibilityTimeout: FiniteDuration = 30.seconds,
                         waitTimeSeconds: FiniteDuration = Duration.Zero): Task[List[DeletableMessage]] = {
    val receiveRequest = singleReceiveRequest(queueUrl,
      maxMessages = maxMessages,
      visibilityTimeout = visibilityTimeout,
      waitTimeSeconds = waitTimeSeconds)
    Task.evalAsync(receiveRequest).flatMap {
      SqsOp.receiveMessage.execute(_)
        .map(_.messages.asScala.toList
          .map(msg => new DeletableMessage(queueUrl, msg)))
    }
  }

  def receiveAutoDelete(queueUrl: QueueUrl,
                        visibilityTimeout: FiniteDuration = 30.seconds,
                        waitTimeSeconds: FiniteDuration = Duration.Zero,
                        autoDeleteRequestRetry: Int = 3): Observable[ReceivedMessage] = {
    receiveManualDelete(queueUrl,
      inFlightMessages = 10,
      visibilityTimeout = visibilityTimeout,
      waitTimeSeconds = waitTimeSeconds)
      .mapEvalF { deletableMessage =>
        deletableMessage.deleteFromQueue()
          .as(deletableMessage)
          .onErrorRestart(autoDeleteRequestRetry)
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
