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

package monix.connect.mongodb.internal

import monix.connect.mongodb.domain.RetryStrategy
import monix.eval.Task
import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.InternalApi
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.reactivestreams.Publisher

import scala.concurrent.Future

//todo mszmal: add doc
@InternalApi
private[mongodb] class MongoSinkParSubscriber[A, B](op: A => Publisher[B], retryStrategy: RetryStrategy)
  extends Consumer[List[A], Unit] {

  override def createSubscriber(
    cb: Callback[Throwable, Unit],
    s: Scheduler): (Subscriber[List[A]], AssignableCancelable) = {
    val sub = new Subscriber[List[A]] {

      implicit val scheduler: Scheduler = s

      def onNext(requests: List[A]): Future[Ack] =
        Task
          .parTraverse(requests)(req => retryOnFailure(op(req), retryStrategy))
          .redeem(ex => {
            onError(ex)
            Ack.Stop
          }, _ => {
            Ack.Continue
          })
          .runToFuture

      def onComplete(): Unit = {
        cb.onSuccess(())
      }

      def onError(ex: Throwable): Unit = {
        cb.onError(ex)
      }
    }
    (sub, AssignableCancelable.single())
  }

}
