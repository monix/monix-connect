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

import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import monix.reactive.{Consumer, Observable}

@deprecated("Moved to `ParquetSource` and `ParquetSink`.", "0.5.0")
object Parquet {

  @deprecated("Moved to `ParquetSource.fromReaderUnsafe`", "0.5.0")
  def reader[T](reader: ParquetReader[T]): Observable[T] =
    new ParquetPublisher[T](reader)

  @deprecated("Moved to `ParquetSink.fromWriterUnsafe`", "0.5.0")
  def writer[T](writer: ParquetWriter[T]): Consumer[T, Long] =
    new ParquetSubscriberUnsafe[T](writer)

}
