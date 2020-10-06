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
import monix.reactive.Observable
import org.apache.parquet.hadoop.ParquetReader

object ParquetSource {

  /**
    * Reads all the records from a single parquet file.
    *
    * @param reader The raw [[ParquetReader]] of type [[T]].
    * @tparam T represents element type of the parquet file to be read
    * @return An [[Observable]] that emits all the read elements of type [[T]].
    */
  @UnsafeBecauseImpure
  def fromReaderUnsafe[T](reader: ParquetReader[T]): Observable[T] =
    new ParquetPublisher[T](reader)

  /**
    * Defines the evaluation of a call to [[ParquetReader]] that will read all
    * from the parquet file and close the created resources.
    *
    * @param readerTask a [[Task]] with the [[ParquetReader]] of type [[T]].
    * @tparam T element type of the parquet file to be read
    * @return An [[Observable]] that emits all the read elements of type [[T]].
    */
  def fromReader[T](readerTask: Task[ParquetReader[T]]): Observable[T] =
    Observable.resource(readerTask)(reader => Task(reader.close())).flatMap(fromReaderUnsafe)

  /**
    * Defines the evaluation of a call to [[ParquetReader]] that will read all
    * from the parquet file and close the created resources.
    *
    * @param readerCoeval a [[Coeval]] with the [[ParquetReader]] of type [[T]].
    * @tparam T the element type of the parquet file to be read
    * @return An [[Observable]] that emits all the read elements of type [[T]].
    */
  def fromReader[T](readerCoeval: Coeval[ParquetReader[T]]): Observable[T] =
    Observable.resource(Task.coeval(readerCoeval))(reader => Task(reader.close())).flatMap(fromReaderUnsafe)
}
