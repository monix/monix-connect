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
import monix.execution.annotations.UnsafeBecauseImpure
import monix.reactive.Observable
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}

object ParquetSource {

  /**
    * Reads the records from a Parquet file.
    *
    * @param reader The apache hadoop generic implementation of a parquet reader.
    *               See the following known implementations of [[ParquetWriter]] for avro and protobuf respectively:
    *               [[org.apache.parquet.avro.AvroParquetWriter]], [[org.apache.parquet.proto.ProtoParquetWriter]].
    * @tparam T A hinder kinded type that represents element type of the parquet file to be read
    * @return All the elements of type [[T]] the specified parquet file as [[Observable]]
    */
  @UnsafeBecauseImpure
  def fromReaderUnsafe[T](reader: ParquetReader[T]): Observable[T] =
    new ParquetPublisher[T](reader)

  /**
    * Reads the records from a Parquet file.
    *
    * @param reader The apache hadoop generic implementation of a parquet reader.
    *               See the following known implementations of [[ParquetWriter]] for avro and protobuf respectively:
    *               [[org.apache.parquet.avro.AvroParquetWriter]], [[org.apache.parquet.proto.ProtoParquetWriter]].
    * @tparam T A hinder kinded type that represents element type of the parquet file to be read
    * @return All the elements of type [[T]] the specified parquet file as [[Observable]]
    */
  def fromReader[T](reader: Task[ParquetReader[T]]): Observable[T] =
    Observable.resource(reader)(reader => Task(reader.close())).flatMap(fromReaderUnsafe)
}
