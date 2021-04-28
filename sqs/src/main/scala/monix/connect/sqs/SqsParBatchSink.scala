/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
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

package monix.connect.sqs

import com.typesafe.scalalogging.StrictLogging
import monix.connect.sqs.SqsParBatchSink.groupMessagesInBatches
import monix.connect.sqs.domain.{InboundMessage, QueueUrl}
import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[sqs] class SqsParBatchSink(queueUrl: QueueUrl,
                                   groupId: Option[String],
                                   delayDuration: Option[FiniteDuration],
                                   asyncClient: SqsAsyncClient,
                                   stopOnError: Boolean)
  extends Consumer[List[InboundMessage], Unit] with StrictLogging {

  override def createSubscriber(cb: Callback[Throwable, Unit], s: Scheduler): (Subscriber[List[InboundMessage]], AssignableCancelable) = {
    val sub = new Subscriber[List[InboundMessage]] {

      implicit val scheduler: Scheduler = s

      def onNext(inboundMessages: List[InboundMessage]): Future[Ack] = {
        Task.parTraverse {
          groupMessagesInBatches(inboundMessages, queueUrl, groupId, delayDuration)
        } { batch =>
          SqsOp.sendMessageBatch.execute(batch)(asyncClient)
        }
          .onErrorRecover {
            case NonFatal(ex) => {
              if (stopOnError) {
                onError(ex)
                val errorMessage = "Unexpected error in SqsParBatchSink, stopping subscription..."
                logger.error(errorMessage, ex)
                Ack.Stop
              }
              else {
                logger.error(s"Unexpected error in SqsParBatchSink, continuing... ", ex)
                Ack.Continue
              }
            }
          }
          .as(Ack.Continue)
          .runToFuture
      }

      def onComplete(): Unit =
        cb.onSuccess(())

      def onError(ex: Throwable): Unit =
        cb.onError(ex)
    }

    (sub, AssignableCancelable.single())
  }

}

object SqsParBatchSink {
  def groupMessagesInBatches(
                              inboundMessages: List[InboundMessage],
                              queueUrl: QueueUrl,
                             groupId: Option[String] = None,
                             delayDuration: Option[FiniteDuration] = None,
                             ): List[SendMessageBatchRequest] = {
    inboundMessages match {
      case Nil => List.empty
      case _ =>
        val (firstBatch, nextBatch) = inboundMessages.splitAt(10)
        val batchEntries = firstBatch.zipWithIndex.map { case (message, index) => message.toMessageBatchEntry(index.toString, groupId, delayDuration) }
        val batchRequest = SendMessageBatchRequest.builder.entries(batchEntries.asJava).queueUrl(queueUrl.url).build
        List(batchRequest) ++ groupMessagesInBatches(nextBatch, queueUrl, groupId, delayDuration)
    }

  }


}
