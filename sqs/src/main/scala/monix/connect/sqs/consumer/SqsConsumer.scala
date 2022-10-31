/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.sqs.consumer

import com.typesafe.scalalogging.StrictLogging
import monix.connect.sqs.domain.QueueUrl
import monix.connect.sqs.SqsOp
import monix.eval.Task
import monix.reactive.Observable
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{QueueAttributeName, ReceiveMessageRequest}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

private[sqs] object SqsConsumer {
  def create(implicit asyncClient: SqsAsyncClient): SqsConsumer = new SqsConsumer()
}

class SqsConsumer private[sqs] (private[sqs] implicit val asyncClient: SqsAsyncClient) extends StrictLogging {

  /**
    * Starts the process of consuming deletable messages from the specified `queueUrl`.
    *
    * A [[DeletableMessage]] provides the control over when the message is considered as
    * processed, thus, can be deleted from the source queue and allow the next message
    * to be consumed.
    *
    * @see [[receiveAutoDelete]] for at most once semantics.
    * @param queueUrl          source queue url
    * @param maxMessages  max number of message to be consumed, which at most can be 10, otherwise
    *                     it will fail.
    *                     The meaning of `maxMessages` can differ when this parameter is applied
    *                     against standard or fifo queues.
    *                     - Standard: it will solely represent the number of messages requested and
    *                     consumed in the same request.
    *                     - Fifo: the max messages would actually represented as `inFlight`
    *                     messages (consumed but not yet deleted), meaning that we would not
    *                     be able to consume further message from the same queue until there
    *                     is a deletion or the `visibilityTimeout` gets expired.
    *                     See the official aws docs for further details.
    * @param visibilityTimeout The duration (in seconds) that the received messages are hidden
    *                          from subsequent retrieve requests after being retrieved (in case they
    *                          have not been deleted before).
    * @param waitTimeSeconds   The duration (in seconds) for which the call waits for a message
    *                          to arrive in the queue before returning. If a message is available,
    *                          the call returns sooner than the wait timeout.
    *                          By default, `waitTimeSeconds` is set to [[Duration.Zero]], aka `short pooling`,
    *                          which means that it will only consume messages available at that time.
    *                          On the other hand, the user can switch to `long pooling` by increasing the
    *                          `waitTimeSeconds` to more than zero, which in that case it will wait for
    *                          new messages to be available.
    *                          See more in docs: https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.
    *                          Notice that in `short polling` (default), if there is no available message
    *                          because the `inFlight` has reached the maximum limits of the queue it will
    *                          return [[software.amazon.awssdk.services.sqs.model.OverLimitException]],
    *                          on the other hand, when using long pooling we would just not receive messages.
    *                          Additionally, ensure that the HTTP response timeout of the [[NettyNioAsyncHttpClient]]
    *                          is longer than the WaitTimeSeconds parameter to avoid timeout errors.
    */
  def receiveManualDelete(
    queueUrl: QueueUrl,
    maxMessages: Int = 10,
    visibilityTimeout: FiniteDuration = 30.seconds,
    waitTimeSeconds: FiniteDuration = Duration.Zero,
    onErrorMaxRetries: Int = 5): Observable[DeletableMessage] = {
    Observable.repeatEvalF {
      receiveSingleManualDelete(
        queueUrl,
        maxMessages = maxMessages,
        visibilityTimeout = visibilityTimeout,
        waitTimeSeconds = waitTimeSeconds,
        onErrorMaxRetries = onErrorMaxRetries)
    }.flatMap(Observable.fromIterable)
  }

  /**
    * Starts the process that keeps consuming messages and deleting them right after.
    *
    * A [[ConsumedMessage]] provides the control over when the message is considered as
    * processed, thus can be deleted from the source queue and allow the next message
    * to be consumed.
    *
    * @see [[receiveManualDelete]] for at least once semantics.
    * @param queueUrl        source queue url
    * @param maxMessages  max number of message to be consumed, which at most can be 10, otherwise
    *                     it will fail.
    *                     The meaning of `maxMessages` can differ when this parameter is applied
    *                     against standard or fifo queues.
    *                     - Standard: it will solely represent the number of messages requested and
    *                     consumed in the same request.
    *                     - Fifo: the max messages would actually represented as `inFlight`
    *                     messages (consumed but not yet deleted), meaning that we would not
    *                     be able to consume further message from the same queue until there
    *                     is a deletion or the `visibilityTimeout` gets expired.
    *                     See the official aws docs for further details.
    * @param waitTimeSeconds The duration (in seconds) for which the call waits for a message
    *                        to arrive in the queue before returning. If a message is available,
    *                        the call returns sooner than the wait timeout.
    *                        By default, `waitTimeSeconds` is set to [[Duration.Zero]], aka `short pooling`,
    *                        which means that it will only consume messages available at that time.
    *                        On the other hand, the user can switch to `long pooling` by increasing the
    *                        `waitTimeSeconds` to more than zero, which in that case it will wait for
    *                        new messages to be available.
    *                        See more in docs: https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.
    *                        Notice that in `short polling` (default), if there is no available message
    *                        because the `inFlight` has reached the maximum limits of the queue it will
    *                        return [[software.amazon.awssdk.services.sqs.model.OverLimitException]],
    *                        on the other hand, when using long pooling we would just not receive messages.
    *                        Additionally, ensure that the HTTP response timeout of the [[NettyNioAsyncHttpClient]]
    *                        is longer than the WaitTimeSeconds parameter to avoid timeout errors.
    */
  def receiveAutoDelete(
    queueUrl: QueueUrl,
    maxMessages: Int = 10,
    waitTimeSeconds: FiniteDuration = Duration.Zero,
    onErrorMaxRetries: Int = 3): Observable[ConsumedMessage] = {
    Observable.repeatEvalF {
      receiveSingleAutoDelete(queueUrl, maxMessages, waitTimeSeconds, onErrorMaxRetries = onErrorMaxRetries)
    }.flatMap(Observable.fromIterable)
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
    * @param queueUrl          source queue url
    * @param maxMessages  max number of message to be consumed, which at most can be 10, otherwise
    *                     it will fail.
    *                     The meaning of `maxMessages` can differ when this parameter is applied
    *                     against standard or fifo queues.
    *                     - Standard: it will solely represent the number of messages requested and
    *                     consumed in the same request.
    *                     - Fifo: the max messages would actually represented as `inFlight`
    *                     messages (consumed but not yet deleted), meaning that we would not
    *                     be able to consume further message from the same queue until there
    *                     is a deletion or the `visibilityTimeout` gets expired.
    *                     See the official aws docs for further details.
    * @param visibilityTimeout The duration (in seconds) that the received messages are hidden
    *                          from subsequent retrieve requests after being retrieved (in case they
    *                          have not been deleted before).
    * @param waitTimeSeconds   The duration (in seconds) for which the call waits for a message
    *                          to arrive in the queue before returning. If a message is available,
    *                          the call returns sooner than the wait timeout.
    *                          By default, `waitTimeSeconds` is set to [[Duration.Zero]], aka `short pooling`,
    *                          which means that it will only consume messages available at that time.
    *                          On the other hand, the user can switch to `long pooling` by increasing the
    *                          `waitTimeSeconds` to more than zero, which in that case it will wait for
    *                          new messages to be available.
    *                          See more in docs: https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.
    *                          Notice that in `short polling` (default), if there is no available message
    *                          because the `inFlight` has reached the maximum limits of the queue it will
    *                          return [[software.amazon.awssdk.services.sqs.model.OverLimitException]],
    *                          on the other hand, when using long pooling we would just not receive messages.
    *                          Additionally, ensure that the HTTP response timeout of the [[NettyNioAsyncHttpClient]]
    *                          is longer than the WaitTimeSeconds parameter to avoid timeout errors.
    *
    * @return a list of [[DeletableMessage]]s limited to the configured `maxMessages` (at most 10)
    *         and the maximum available in the queue at the given request time.
    */
  def receiveSingleManualDelete(
    queueUrl: QueueUrl,
    maxMessages: Int = 10,
    visibilityTimeout: FiniteDuration = 30.seconds,
    waitTimeSeconds: FiniteDuration = Duration.Zero,
    onErrorMaxRetries: Int = 1): Task[List[DeletableMessage]] = {
    val receiveRequest = singleReceiveRequest(
      queueUrl,
      maxMessages = maxMessages,
      visibilityTimeout = visibilityTimeout,
      waitTimeSeconds = waitTimeSeconds)
    Task
      .evalAsync(receiveRequest)
      .flatMap {
        SqsOp.receiveMessage
          .execute(_)
          .map(_.messages.asScala.toList
            .map(msg => new DeletableMessage(queueUrl, msg)))
      }
      .onErrorRestart(onErrorMaxRetries)
      .onErrorHandleWith { ex =>
        logger.error("Receive manual delete failed unexpectedly.", ex)
        Task.raiseError(ex)
      }
  }

  /**
    * Single receive message task that responds with a list of
    * **already** deleted messages, aka [[ConsumedMessage]],
    * from the specified `queueUrl`.
    *
    * Meaning that the semantics that this method provides are **at most once**,
    * since the message is automatically deleted right after being consumed,
    * in case there is a failure during the processing of the message
    * it would be lost as it could not be read again from the queue.
    *
    * @see [[receiveSingleManualDelete]] for at least once semantics.
    * @param queueUrl        source queue url
    * @param maxMessages  max number of message to be consumed, which at most can be 10, otherwise
    *                     it will fail.
    *                     The meaning of `maxMessages` can differ when this parameter is applied
    *                     against standard or fifo queues.
    *                     - Standard: it will solely represent the number of messages requested and
    *                     consumed in the same request.
    *                     - Fifo: the max messages would actually represented as `inFlight`
    *                     messages (consumed but not yet deleted), meaning that we would not
    *                     be able to consume further message from the same queue until there
    *                     is a deletion or the `visibilityTimeout` gets expired.
    *                     See the official aws docs for further details.
    * @param waitTimeSeconds The duration (in seconds) for which the call waits for a message to arrive
    *                        in the queue before returning. If a message is available, the call returns
    *                        sooner than WaitTimeSeconds. If no messages are available and the wait time expires,
    *                        the call returns successfully with an empty list of messages.
    *                        Ensure that the HTTP response timeout of the [[NettyNioAsyncHttpClient]]
    *                        is longer than the WaitTimeSeconds parameter to avoid errors.
    * @return a list of [[ConsumedMessage]]s of at most 10 messages (maximum configurable per request)
    */
  def receiveSingleAutoDelete(
    queueUrl: QueueUrl,
    maxMessages: Int = 10,
    waitTimeSeconds: FiniteDuration = Duration.Zero,
    onErrorMaxRetries: Int = 1): Task[List[ConsumedMessage]] = {
    receiveSingleAutoDeleteInternal(queueUrl, maxMessages, waitTimeSeconds = waitTimeSeconds)
      .onErrorRestart(onErrorMaxRetries)
      .onErrorHandleWith { ex =>
        logger.error("Receive auto delete failed unexpectedly.", ex)
        Task.raiseError(ex)
      }
  }

  private[sqs] def receiveSingleAutoDeleteInternal(
    queueUrl: QueueUrl,
    maxMessages: Int,
    waitTimeSeconds: FiniteDuration = Duration.Zero,
    visibilityTimeout: FiniteDuration = 30.seconds): Task[List[ConsumedMessage]] = {
    receiveSingleManualDelete(
      queueUrl,
      maxMessages,
      waitTimeSeconds = waitTimeSeconds,
      visibilityTimeout = visibilityTimeout)
      .tapEval(Task.traverse(_)(_.deleteFromQueue()))
  }

  private[this] def singleReceiveRequest(
    queueUrl: QueueUrl,
    maxMessages: Int,
    visibilityTimeout: FiniteDuration,
    waitTimeSeconds: FiniteDuration = Duration.Zero): ReceiveMessageRequest = {
    val builder = ReceiveMessageRequest.builder
      .queueUrl(queueUrl.url)
      .maxNumberOfMessages(maxMessages)
      .attributeNames(QueueAttributeName.ALL)
      .visibilityTimeout(visibilityTimeout.toSeconds.toInt)
    if waitTimeSeconds.toSeconds > 0L then builder.waitTimeSeconds(waitTimeSeconds.toSeconds.toInt)
    builder.build
  }

  private[sqs] def close: Task[Unit] = Task.evalAsync(asyncClient.close())

}
