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

package monix.connect.sqs.inbound

import com.typesafe.scalalogging.StrictLogging
import monix.connect.sqs.SqsOp
import monix.connect.sqs.domain.QueueUrl
import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SendMessageRequest, SendMessageResponse, SqsRequest, SqsResponse}

import scala.concurrent.Future

class SqsSink[In, Request <: SqsRequest, Response <: SqsResponse] private[sqs](preProcessing: In => Request,
                                                                               sqsOp: SqsOp[Request, Response],
                                                                               onErrorHandleWith: Throwable => Task[Ack])
                                                                              (implicit sqsClient: SqsAsyncClient)
  extends Consumer[In, Unit] with StrictLogging {

  override def createSubscriber(cb: Callback[Throwable, Unit], s: Scheduler): (Subscriber[In], AssignableCancelable) = {
    val sub = new Subscriber[In] {

      implicit val scheduler: Scheduler = s

      def onNext(sqsRequest: In): Future[Ack] = {
        sqsOp.execute(preProcessing(sqsRequest))(sqsClient)
          .onErrorHandleWith(onErrorHandleWith)
          .as(Ack.Continue)
          .runToFuture
      }

      def onComplete(): Unit =
        cb.onSuccess(())

      def onError(ex: Throwable): Unit = {
        logger.error("Unexpected error in SqsSink.", ex)
        cb.onError(ex)
      }
    }

    (sub, AssignableCancelable.single())
  }

}

object SqsSink {

  def send(queueUrl: QueueUrl,
           sqsOp: SqsOp[SendMessageRequest, SendMessageResponse],
           onErrorHandleWith: Throwable => Task[Ack])
           (implicit asyncClient: SqsAsyncClient): Consumer[InboundMessage, Unit] = {
    val toJavaMessage = (message: InboundMessage) => message.toMessageRequest(queueUrl)
    new SqsSink(toJavaMessage, sqsOp, onErrorHandleWith)
  }
}
