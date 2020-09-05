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
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.parquet.hadoop.ParquetWriter

import scala.util.control.NonFatal

/**
  * A parquet writer implemented as a [[Subscriber.Sync]].
  * @param parquetWriter The apache hadoop generic implementation of a parquet writer.
  * @tparam T Represents the type of the elements that will be written into the parquet file.
  */
class ParquetSubscriber[T](parquetWriter: ParquetWriter[T]) extends Consumer.Sync[T, Long] {

  def createSubscriber(
    callback: Callback[Throwable, Long],
    s: Scheduler): (Subscriber.Sync[T], AssignableCancelable) = {
    val out = new Subscriber.Sync[T] {
      override implicit def scheduler: Scheduler = s

      //the number of parquet files that has been written that is returned as materialized value
      var nElements: Long = 0
      override def onNext(record: T): Ack = {
        try {
          parquetWriter.write(record)
          nElements = nElements + 1
          monix.execution.Ack.Continue
        } catch {
          case ex if NonFatal(ex) => {
            onError(ex)
            Ack.Stop
          }
        }
      }

      override def onComplete() = {
        parquetWriter.close()
        callback.onSuccess(nElements)
      }

      override def onError(ex: Throwable): Unit = {
        parquetWriter.close()
        callback.onError(ex)
      }
    }

    (out, AssignableCancelable.dummy)
  }

}
