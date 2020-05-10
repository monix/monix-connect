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

import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}

object Parquet {

  def writer[T](writer: ParquetWriter[T])(implicit scheduler: Scheduler): Consumer[T, Task[Unit]] = {
    new ParquetSubscriber[T](writer)
  }

  def reader[T](reader: ParquetReader[T])(implicit scheduler: Scheduler): Observable[T] = {
    ParquetPublisher(reader).create
  }

}
