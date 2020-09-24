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
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}

object Parquet {

  /**
    * Writes records to a Parquet file.
    *
    * @param writer The apache hadoop generic implementation of a parquet writer.
    *               See the following known implementations of [[ParquetWriter]] for avro and protobuf respectively:
    *               [[org.apache.parquet.avro.AvroParquetWriter]], [[org.apache.parquet.proto.ProtoParquetWriter]].
    * @param scheduler An implicit [[Scheduler]] instance to be in the scope of the call.
    * @tparam T A hinder kinded type that represents the element type of the parquet file to be written.
    * @return A [[Consumer]] that expects records of type [[T]] to be passed and materializes to [[Long]]
    *         that represents the number of elements written.
    */
  def writer[T](writer: ParquetWriter[T])(implicit scheduler: Scheduler): Consumer[T, Long] = {
    new ParquetSubscriber[T](writer)
  }

  /**
    * Reads the records from a Parquet file.
    *
    * @param reader The apache hadoop generic implementation of a parquet reader.
    *               See the following known implementations of [[ParquetWriter]] for avro and protobuf respectively:
    *               [[org.apache.parquet.avro.AvroParquetWriter]], [[org.apache.parquet.proto.ProtoParquetWriter]].
    * @param scheduler An implicit [[Scheduler]] instance to be in the scope of the call.
    * @tparam T A hinder kinded type that represents element type of the parquet file to be read.
    * @return All the elements of type [[T]] the specified parquet file as [[Observable]]
    */
  def fromReaderUnsafe[T](reader: ParquetReader[T])(implicit scheduler: Scheduler): Observable[T] = {
    new ParquetPublisher[T](reader)
  }
}
