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
import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.InternalApi
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.reactivestreams.Publisher

import scala.concurrent.Future

/**
  * A pre-built Monix [[Consumer]] implementation representing a Sink that expects events
  * of type [[A]] and executes the mongodb operation [[op]] passed to the class constructor.
  *
  * @param op                the mongodb operation defined as that expects an event of type [[A]] and
  *                          returns a reactivestreams [[Publisher]] of [[Any]].
  * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
  * @tparam A the type that the [[Consumer]] expects to receive
  */
@InternalApi private[mongodb] class MongoSinkSubscriber[A, B](op: A => Publisher[B], retryStrategy: RetryStrategy)
  extends Consumer[A, Unit] {

  override def createSubscriber(cb: Callback[Throwable, Unit], s: Scheduler): (Subscriber[A], AssignableCancelable) = {
    val sub = new Subscriber[A] {

      implicit val scheduler = s

      def onNext(request: A): Future[Ack] = {
        retryOnFailure(op(request), retryStrategy)
          .redeem(ex => {
            onError(ex)
            Ack.Stop
          }, _ => {
            Ack.Continue
          })
          .runToFuture
      }

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
