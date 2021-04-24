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

import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.Consumer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SqsRequest, SqsResponse}
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.Future
import scala.util.control.NonFatal

private[sqs] class SqsSink[In, Request <: SqsRequest, Response <: SqsResponse](
                                                                                preProcessing: In => Request,
                                                                  sqsOp: SqsOp[Request, Response],
                                                                  sqsClient: SqsAsyncClient,
                                                                  stopOnError: Boolean)
  extends Consumer[In, Unit] with StrictLogging {

  override def createSubscriber(cb: Callback[Throwable, Unit], s: Scheduler): (Subscriber[In], AssignableCancelable) = {
    val sub = new Subscriber[In] {

      implicit val scheduler: Scheduler = s

      def onNext(sqsRequest: In): Future[Ack] = {

        sqsOp.execute(preProcessing(sqsRequest))(sqsClient)
          .onErrorRecover {
            case NonFatal(ex) => {
              if (stopOnError) {
                onError(ex)
                val errorMessage = "Unexpected error in SqsSink, stopping subscription..."
                logger.error(errorMessage, ex)
                Ack.Stop
              }
              else {
                logger.error(s"Unexpected error in SqsSink, continuing... ", ex)
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
