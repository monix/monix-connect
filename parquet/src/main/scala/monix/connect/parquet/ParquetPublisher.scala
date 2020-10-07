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

package monix.connect.parquet

import monix.execution.Ack.{Continue, Stop}
import monix.execution.cancelables.BooleanCancelable
import monix.execution.{Ack, Cancelable, ExecutionModel, Scheduler}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import org.apache.parquet.hadoop.ParquetReader

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * A builder for creating a reactive parquet reader as [[Observable]].
  * @param reader the underlying apache hadoop generic implementation of a parquet reader.
  */
private[parquet] final class ParquetPublisher[T](reader: ParquetReader[T]) extends Observable[T] {

  def unsafeSubscribeFn(subscriber: Subscriber[T]): Cancelable = {
    val s = subscriber.scheduler
    val cancelable = BooleanCancelable()
    fastLoop(subscriber, cancelable, s.executionModel, 0)(s)
    cancelable
  }

  @tailrec
  def fastLoop(o: Subscriber[T], c: BooleanCancelable, em: ExecutionModel, syncIndex: Int)(
    implicit
    s: Scheduler): Unit = {

    val ack =
      try {
        val r = reader.read()
        if (r != null) o.onNext(r)
        else {
          o.onComplete()
          Stop
        }
      } catch {
        case ex if NonFatal(ex) =>
          Future.failed(ex)
      }

    val nextIndex =
      if (ack == Continue) em.nextFrameIndex(syncIndex)
      else if (ack == Stop) -1
      else 0

    if (nextIndex > 0)
      fastLoop(o, c, em, nextIndex)
    else if (nextIndex == 0 && !c.isCanceled)
      reschedule(ack, o, c, em)
  }

  def reschedule(ack: Future[Ack], o: Subscriber[T], c: BooleanCancelable, em: ExecutionModel)(
    implicit
    s: Scheduler): Unit =
    ack.onComplete {
      case Success(success) =>
        if (success == Continue) fastLoop(o, c, em, 0)
      case Failure(ex) =>
        o.onError(ex)
        s.reportFailure(ex)
      case _ =>
        () // this was a Stop, do nothing
    }
}
