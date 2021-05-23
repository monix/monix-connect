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

package monix.connect.sqs.outbound

import com.typesafe.scalalogging.StrictLogging
import monix.connect.sqs.domain.QueueUrl
import monix.connect.sqs.{SqsOp, SqsOperator}
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
    * @param queueUrl          source queue url, obtained from [[SqsOperator.getQueueUrl]].
    * @param inFlightMessages  max number of message to be consumed at a time, meaning that
    *                          we would not be able to consume further message from the same
    *                          queue and groupId until at least one of the previous batch
    *                          was deleted from the queue.
    * @param visibilityTimeout The duration (in seconds) that the received messages are hidden
    *                          from subsequent retrieve requests after being retrieved (in case they
    *                          have not been deleted before).
    * @param waitTimeSeconds   The duration (in seconds) for which the call waits for a message
    *                          to arrive in the queue before returning. If a message is available,
    *                          the call returns sooner than WaitTimeSeconds.
    *                          If no messages are available and the wait time expires,
    *                          the call returns successfully with an empty list of messages.
    *                          Ensure that the HTTP response timeout of the [[NettyNioAsyncHttpClient]]
    *                          is longer than the WaitTimeSeconds parameter to avoid errors.
    */
  def receiveManualDelete(
    queueUrl: QueueUrl,
    inFlightMessages: Int = 10,
    visibilityTimeout: FiniteDuration = 30.seconds,
    waitTimeSeconds: FiniteDuration = Duration.Zero,
    onErrorMaxRetries: Int = 5): Observable[DeletableMessage] = {
    Observable.repeatEvalF {
      receiveSingleManualDelete(
        queueUrl,
        inFlightMessages = inFlightMessages,
        visibilityTimeout = visibilityTimeout,
        waitTimeSeconds = waitTimeSeconds)
        .onErrorRestart(onErrorMaxRetries)
        .onErrorHandleWith { ex =>
          logger.error("Receive manual delete failed unexpectedly.", ex)
          Task.raiseError(ex)
        }
    }.flatMap { deletableMessages => Observable.fromIterable(deletableMessages) }
  }

  /**
    * Starts the process that keeps consuming messages and deleting them right after.
    *
    * A [[ReceivedMessage]] provides the control over when the message is considered as
    * processed, thus can be deleted from the source queue and allow the next message
    * to be consumed.
    *
    * @see [[receiveManualDelete]] for at least once semantics.
    * @param queueUrl        source queue url, obtained from [[SqsOperator.getQueueUrl]].
    * @param waitTimeSeconds The duration (in seconds) for which the call waits for a message
    *                        to arrive in the queue before returning. If a message is available,
    *                        the call returns sooner than WaitTimeSeconds.
    *                        If no messages are available and the wait time expires,
    *                        the call returns successfully with an empty list of messages.
    *                        Ensure that the HTTP response timeout of the [[NettyNioAsyncHttpClient]]
    *                        is longer than the WaitTimeSeconds parameter to avoid errors.
    */
  def receiveAutoDelete(
    queueUrl: QueueUrl,
    waitTimeSeconds: FiniteDuration = Duration.Zero,
    onErrorMaxRetries: Int = 3): Observable[ReceivedMessage] = {
    receiveManualDelete(
      queueUrl,
      // 10 as being the maximum configurable that would give
      // the highest performance, still transparent to the user.
      inFlightMessages = 10,
      // visibility timeout does not apply to already deleted messages
      visibilityTimeout = 30.seconds,
      waitTimeSeconds = waitTimeSeconds
    ).onErrorRestart(onErrorMaxRetries).mapEvalF { deletableMessage =>
      deletableMessage
        .deleteFromQueue()
        .as(deletableMessage)
        .onErrorRestart(onErrorMaxRetries)
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
    * @param queueUrl          source queue url, obtained from [[SqsOperator.getQueueUrl]].
    * @param inFlightMessages  max number of message to be consumed at a time, at most 10!
    *                          meaning that we would not be able to consume further message
    *                          from the same queue and groupId until at least one of the
    *                          previous batch was deleted from the queue.
    *                          **Important** the request will fail for numbers higher than 10.
    * @param visibilityTimeout The duration (in seconds) that the received messages are hidden
    *                          from subsequent retrieve requests after being retrieved (in case they
    *                          have not been deleted before).
    * @param waitTimeSeconds   The duration (in seconds) for which the call waits for a message to arrive
    *                          in the queue before returning. If a message is available, the call returns
    *                          sooner than WaitTimeSeconds. If no messages are available and the wait time expires,
    *                          the call returns successfully with an empty list of messages.
    *                          Ensure that the HTTP response timeout of the [[NettyNioAsyncHttpClient]]
    *                          is longer than the WaitTimeSeconds parameter to avoid errors.
    *
    * @return a list of [[DeletableMessage]]s limited to `inFlightMessages` and the maximum available
    *         in the queue at that time.
    */
  def receiveSingleManualDelete(
    queueUrl: QueueUrl,
    inFlightMessages: Int = 10,
    visibilityTimeout: FiniteDuration = 30.seconds,
    waitTimeSeconds: FiniteDuration = Duration.Zero): Task[List[DeletableMessage]] = {
    val receiveRequest = singleReceiveRequest(
      queueUrl,
      maxMessages = inFlightMessages,
      visibilityTimeout = visibilityTimeout,
      waitTimeSeconds = waitTimeSeconds)
    Task.evalAsync(receiveRequest).flatMap {
      SqsOp.receiveMessage
        .execute(_)
        .map(_.messages.asScala.toList
          .map(msg => new DeletableMessage(queueUrl, msg)))
    }
  }

  /**
    * Single receive message task that responds with a list of
    * **already** deleted messages, aka [[ReceivedMessage]],
    * from the specified `queueUrl`.
    *
    * Meaning that the semantics that this method provides are **at most once**,
    * since the message is automatically deleted right after being consumed,
    * in case there is a failure during the processing of the message
    * it would be lost as it could not be read again from the queue.
    *
    * @see [[receiveSingleManualDelete]] for at least once semantics.
    *
    * @param queueUrl        source queue url
    * @param waitTimeSeconds The duration (in seconds) for which the call waits for a message to arrive
    *                        in the queue before returning. If a message is available, the call returns
    *                        sooner than WaitTimeSeconds. If no messages are available and the wait time expires,
    *                        the call returns successfully with an empty list of messages.
    *                        Ensure that the HTTP response timeout of the [[NettyNioAsyncHttpClient]]
    *                        is longer than the WaitTimeSeconds parameter to avoid errors.
    *
    * @return a list of [[ReceivedMessage]]s of at most 10 messages (maximum configurable per request)
    */
  def receiveSingleAutoDelete(
    queueUrl: QueueUrl,
    waitTimeSeconds: FiniteDuration = Duration.Zero): Task[List[ReceivedMessage]] = {
    receiveSingleAutoDeleteInternal(queueUrl, waitTimeSeconds = waitTimeSeconds)
  }

  private[sqs] def receiveSingleAutoDeleteInternal(
    queueUrl: QueueUrl,
    waitTimeSeconds: FiniteDuration = Duration.Zero,
    visibilityTimeout: FiniteDuration = 30.seconds): Task[List[ReceivedMessage]] = {
    receiveSingleManualDelete(queueUrl, waitTimeSeconds = waitTimeSeconds, visibilityTimeout = visibilityTimeout)
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
    if (waitTimeSeconds.toSeconds > 0L) builder.waitTimeSeconds(waitTimeSeconds.toSeconds.toInt)
    builder.build
  }

  private[sqs] def close: Task[Unit] = Task.evalAsync(asyncClient.close())

}
