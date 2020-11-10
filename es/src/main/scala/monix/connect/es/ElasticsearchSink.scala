/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

package monix.connect.es

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.requests.bulk.BulkCompatibleRequest
import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.InternalApi
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber

import scala.concurrent.Future

/**
  *  A pre-built [[Consumer]] implementation that expects incoming [[BulkCompatibleRequest]]
  *
  * @param es an instance of a [[Elasticsearch]]
  */
@InternalApi private[es] class ElasticsearchSink(es: Elasticsearch) extends Consumer[Seq[BulkCompatibleRequest], Unit] {
  override def createSubscriber(
    cb: Callback[Throwable, Unit],
    s: Scheduler
  ): (Subscriber[Seq[BulkCompatibleRequest]], AssignableCancelable) = {

    val subscriber = new Subscriber[Seq[BulkCompatibleRequest]] {
      override implicit def scheduler: Scheduler = s

      override def onNext(elem: Seq[BulkCompatibleRequest]): Future[Ack] = {
        es.bulkExecuteRequest(elem)
          .map {
            case RequestSuccess(_, _, _, _) =>
              Ack.Continue
            case RequestFailure(_, _, _, error) =>
              onError(error.asException)
              Ack.Stop
          }
          .runToFuture
      }

      override def onError(ex: Throwable): Unit = {
        cb.onError(ex)
      }

      override def onComplete(): Unit = {
        cb.onSuccess()
      }
    }
    (subscriber, AssignableCancelable.single())
  }
}
