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

import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, CancelableFuture, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.Consumer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SqsRequest, SqsResponse}

import scala.concurrent.Future
import scala.util.control.NonFatal

private[sqs] class SqsSink[In <: SqsRequest, Out <: SqsResponse]()(
  implicit
  sqsOp: SqsOp[In, Out],
  client: SqsAsyncClient)
  extends Consumer[In, Out] {

  override def createSubscriber(cb: Callback[Throwable, Out], s: Scheduler): (Subscriber[In], AssignableCancelable) = {
    val sub = new Subscriber[In] {

      implicit val scheduler = s
      private var sqsResponse: Task[Out] = _

      def onNext(sqsRequest: In): Future[Ack] = {
        Task
          .from(sqsOp.execute(sqsRequest))
          .redeem(ex => {
            onError(ex)
            Ack.Stop
          }, _ => {
            Ack.Continue
          })
          .runToFuture
      }

      def onComplete(): Unit = {
        sqsResponse.runAsync(cb)
      }

      def onError(ex: Throwable): Unit = {
        cb.onError(ex)
      }
    }
    (sub, AssignableCancelable.single())
  }

}
