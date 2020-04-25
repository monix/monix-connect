/*
 * Copyright (c) 2014-2020 by The Monix Project Developers.
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
import monix.reactive.{Observable, OverflowStrategy}
import monix.reactive.observers.Subscriber
import org.apache.parquet.hadoop.ParquetReader

import monix.execution.Ack
private[parquet] class ParquetPublisher[T](reader: ParquetReader[T]) {

  private def readRecords(sub: Subscriber[T]): Task[Unit] = {
    val t = Task(reader.read())
    t.flatMap { r =>
      if (r != null) {
        Task.deferFuture(sub.onNext(r)).flatMap {
          case Ack.Continue => readRecords(sub)
          case Ack.Stop => Task.unit
        }
      } else {
        sub.onComplete()
        Task.unit
      }
    }
  }

  val create: Observable[T] =
    Observable.create(OverflowStrategy.Unbounded) { sub => readRecords(sub).runToFuture(sub.scheduler) }
}

object ParquetPublisher {
  def apply[T](reader: ParquetReader[T]): ParquetPublisher[T] = new ParquetPublisher(reader)
}
