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

package monix.connect.sqs.inbound

import monix.connect.sqs.domain.QueueUrl
import monix.connect.sqs.SqsOp
import monix.connect.sqs.inbound.SqsParBatchSink.groupMessagesInBatches
import monix.eval.Task
import monix.execution.Ack
import monix.execution.Ack.Stop
import monix.reactive.Consumer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SendMessageBatchResponse, SendMessageResponse}

class SqsProducer private[sqs] (private[sqs] implicit val asyncClient: SqsAsyncClient) {

  /** Sends a single message to the specified queue. */
  def sendSingleMessage(message: InboundMessage, queueUrl: QueueUrl): Task[SendMessageResponse] = {
    val producerMessage = message.toMessageRequest(queueUrl)
    SqsOp.sendMessage.execute(producerMessage)(asyncClient)
  }

  /**
    * Sends a list of `n` messages in parallel.
    *
    * By design the Sqs batch send request accepts at most 10 entries,
    * but the [[sendParBatch]] implementation overcomes that rule
    * by splitting long given lists of `messages` in batches of 10.
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
  def sendParBatch(messages: List[InboundMessage], queueUrl: QueueUrl): Task[List[SendMessageBatchResponse]] = {
    if (messages.nonEmpty) {
      Task.parTraverse {
        groupMessagesInBatches(messages, queueUrl)
      } { batch => SqsOp.sendMessageBatch.execute(batch)(asyncClient) }
    } else {
      Task.now(List.empty)
    }
  }

  /**
    * [[Consumer]] that listens for [[InboundMessage]]s and produces them at a time
    * to the specified `queueUrl`.
    *
    * @param queueUrl target queue url
    * @param onErrorHandleWith provides the power to the user to decide what to do in
    *                          case there was an error producing a message to sqs.
    *                          by default it [[Stop]]s, it is recommended to create
    *                          a custom logic that satisfies the business needs, like
    *                          logging the error, increasing failed metrics count, etc.
    */
  def sendSink(
    queueUrl: QueueUrl,
    onErrorHandleWith: Throwable => Task[Ack] = _ => Task.pure(Stop)): Consumer[InboundMessage, Unit] =
    SqsSink.send(queueUrl, SqsOp.sendMessage, onErrorHandleWith)

  /**
    * [[Consumer]] that listens for lists [[InboundMessage]]s and produces
    * them in parallel batches to the specified `queueUrl`.
    *
    * The user does not have to worry about the limitation of the
    * aws sdk that restricts the batch size to 10 messages,
    * since each received list of [[InboundMessage]]s  will be split
    * in batches of at most 10 entries, and they will be produced in
    * parallel to sqs.
    *
    * @param queueUrl target queue url
    * @param onErrorHandleWith provides the power to the user to decide what to do in
    *                          case there was an error producing a message to sqs.
    *                          by default it [[Stop]]s, it is recommended to create
    *                          a custom logic that satisfies the business needs, like
    *                          logging the error, increasing failed metrics count, etc.
    */
  def sendParBatchSink(
    queueUrl: QueueUrl,
    onErrorHandleWith: Throwable => Task[Ack] = _ => Task.pure(Stop)): Consumer[List[InboundMessage], Unit] =
    new SqsParBatchSink(queueUrl, onErrorHandleWith)

}

private[sqs] object SqsProducer {
  def create(implicit asyncClient: SqsAsyncClient): SqsProducer = new SqsProducer()
}
