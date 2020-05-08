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

package monix.connect.parquet

import monix.eval.Task
import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter

class ParquetSubscriber[T](parquetWriter: ParquetWriter[T]) extends Consumer.Sync[T, Task[Unit]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[Unit]],
    s: Scheduler): (Subscriber.Sync[T], AssignableCancelable) = {
    val out = new Subscriber.Sync[T] {
      override implicit def scheduler: Scheduler = s

      override def onComplete() = {
        parquetWriter.close()
        callback.onSuccess(Task())
      }

      override def onError(ex: Throwable): Unit = {
        parquetWriter.close()
        callback.onError(ex)
      }

      override def onNext(record: T): Ack = {
        parquetWriter.write(record)
        monix.execution.Ack.Continue
      }
    }

    (out, AssignableCancelable.dummy)
  }

}
