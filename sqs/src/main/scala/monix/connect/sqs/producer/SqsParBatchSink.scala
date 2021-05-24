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

package monix.connect.sqs.producer

import com.typesafe.scalalogging.StrictLogging
import monix.connect.sqs.SqsOp
import monix.connect.sqs.domain.QueueUrl
import monix.connect.sqs.producer.SqsParBatchSink.groupMessagesInBatches
import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest

import scala.concurrent.Future
import scala.jdk.CollectionConverters._

private[sqs] class SqsParBatchSink(queueUrl: QueueUrl, onErrorHandleWith: Throwable => Task[Ack])(
  implicit asyncClient: SqsAsyncClient)
  extends Consumer[List[Message], Unit] with StrictLogging {

  override def createSubscriber(
    cb: Callback[Throwable, Unit],
    s: Scheduler): (Subscriber[List[Message]], AssignableCancelable) = {
    val sub = new Subscriber[List[Message]] {

      implicit val scheduler: Scheduler = s

      def onNext(messages: List[Message]): Future[Ack] = {
        Task
          .parTraverse(groupMessagesInBatches(messages, queueUrl))(SqsOp.sendMessageBatch.execute)
          .onErrorHandleWith(onErrorHandleWith)
          .as(Ack.Continue)
          .runToFuture
      }

      def onComplete(): Unit =
        cb.onSuccess(())

      def onError(ex: Throwable): Unit = {
        logger.error("Unexpected error in SqsParBatchSink.", ex)
        cb.onError(ex)
      }
    }

    (sub, AssignableCancelable.dummy)
  }

}

private[producer] object SqsParBatchSink {
  def groupMessagesInBatches(
    messages: List[Message],
    queueUrl: QueueUrl
  ): List[SendMessageBatchRequest] = {
    messages match {
      case Nil => List.empty
      case _ =>
        val (firstBatch, nextBatch) = messages.splitAt(10)
        val batchEntries = firstBatch.zipWithIndex.map {
          case (message, index) => message.toMessageBatchEntry(index.toString)
        }
        val batchRequest = SendMessageBatchRequest.builder.entries(batchEntries.asJava).queueUrl(queueUrl.url).build
        List(batchRequest) ++ groupMessagesInBatches(nextBatch, queueUrl)
    }

  }

}
