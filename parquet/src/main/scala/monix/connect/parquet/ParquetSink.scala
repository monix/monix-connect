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

import monix.eval.{Coeval, Task}
import monix.execution.annotations.UnsafeBecauseImpure
import monix.reactive.Consumer
import org.apache.parquet.hadoop.ParquetWriter

object ParquetSink {

  /**
    * Writes all emitted elements to a parquet file.
    *
    * @param writer the apache hadoop generic implementation of a parquet writer.
    * @tparam T A hinder kinded type that represents the element type of the parquet file to be written.
    * @return A [[Consumer]] that expects records of type [[T]] to be passed and materializes to [[Long]]
    *         that represents the number of elements written.
    */
  @UnsafeBecauseImpure
  def fromWriterUnsafe[T](writer: ParquetWriter[T]): Consumer[T, Long] =
    new ParquetSubscriberUnsafe[T](writer)

  /**
    * Writes all emitted elements to a parquet file.
    *
    * @param writer The apache hadoop generic implementation of a parquet writer.
    * @tparam T A hinder kinded type that represents the element type of the parquet file to be written.
    * @return A [[Consumer]] that expects records of type [[T]] to be passed and materializes to [[Long]]
    *         that represents the number of elements written.
    */
  def fromWriter[T](writer: Task[ParquetWriter[T]]): Consumer[T, Long] =
    new ParquetSubscriberEval[T](writer)

  /**
    * Writes all emitted elements to a parquet file.
    *
    * @param writer The apache hadoop generic implementation of a parquet writer.
    * @tparam T A hinder kinded type that represents the element type of the parquet file to be written.
    * @return A [[Consumer]] that expects records of type [[T]] to be passed and materializes to [[Long]]
    *         that represents the number of elements written.
    */
  def fromWriter[T](writer: Coeval[ParquetWriter[T]]): Consumer[T, Long] =
    new ParquetSubscriberCoeval[T](writer)

}
