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

import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.execution.internal.{InternalApi, Platform}
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.parquet.hadoop.ParquetWriter

import scala.util.control.NonFatal

/**
  * A [[Consumer]] that writes each emitted element into the same parquet file.
  *
  * @param parquetWriter the underlying apache hadoop [[ParquetWriter]] implementation.
  * @tparam T Represents the type of the elements that will be written into the parquet file.
  */
@InternalApi
private[parquet] class ParquetSubscriberUnsafe[T](parquetWriter: ParquetWriter[T]) extends Consumer[T, Long] {

  def createSubscriber(callback: Callback[Throwable, Long], s: Scheduler): (Subscriber[T], AssignableCancelable) = {
    val out = new Subscriber[T] {
      override implicit val scheduler: Scheduler = s

      //the number of parquet files that has been written that is returned as materialized value
      private[this] var nElements: Long = 0

      // Protects from the situation where last onNext throws, we call onError and then
      // upstream calls onError or onComplete again
      private[this] var isDone = false

      override def onNext(record: T): Ack = {
        try {
          parquetWriter.write(record)
          nElements = nElements + 1
          Ack.Continue
        } catch {
          case ex if NonFatal(ex) =>
            onError(ex)
            Ack.Stop
        }
      }

      override def onComplete(): Unit =
        if (!isDone) {
          isDone = true
          try {
            parquetWriter.close()
            callback.onSuccess(nElements)
          } catch {
            case NonFatal(ex) =>
              callback.onError(ex)
          }
        }

      override def onError(ex: Throwable): Unit =
        if (!isDone) {
          isDone = true
          try {
            parquetWriter.close()
            callback.onError(ex)
          } catch {
            case NonFatal(ex2) =>
              callback.onError(Platform.composeErrors(ex, ex2))
          }
        }
    }

    (out, AssignableCancelable.dummy)
  }

}
