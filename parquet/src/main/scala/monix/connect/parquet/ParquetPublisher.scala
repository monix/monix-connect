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

import monix.eval.Task
import monix.reactive.{Observable, OverflowStrategy}
import monix.reactive.observers.Subscriber
import org.apache.parquet.hadoop.ParquetReader
import monix.execution.Ack

/**
  * The implementation of a reactive parquet publisher.
  * @param reader The apache hadoop generic implementation of a parquet reader.
  */
private[parquet] class ParquetPublisher[T](reader: ParquetReader[T]) {

  /**
    * A tailrec function that reads a record of a parquet file until the last one.
    * @param sub The subscriber to feed with the parquet records read.
    */
  private def readRecords(sub: Subscriber[T]): Task[Unit] = {
    val t = Task(reader.read())
    t.redeemWith(
      ex => Task(sub.onError(ex)),
      r => {
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
    )
  }

  /**
    * Parquet reader [[Observable]] builder.
    */
  val create: Observable[T] =
    Observable.create(OverflowStrategy.Unbounded) { sub => readRecords(sub).runToFuture(sub.scheduler) }
}

/**
  * Companion object builder for [[ParquetPublisher]].
  */
object ParquetPublisher {
  private[parquet] def apply[T](reader: ParquetReader[T]): ParquetPublisher[T] = new ParquetPublisher(reader)
}
