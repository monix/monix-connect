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

package monix.connect.benchmarks.parquet

import java.io.File

import monix.connect.parquet.ParquetSink
import monix.eval.{Coeval, Task}
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.openjdk.jmh.annotations.{BenchmarkMode, Fork, Measurement, Mode, Scope, State, Threads, Warmup, _}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.commons.io.FileUtils

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@Measurement(iterations = 5)
@Warmup(iterations = 1)
@Fork(1)
@Threads(5)
class ParquetWriterBenchmark extends ParquetBenchFixture {

  var size: Int = 250
  val s = Scheduler.io("parquet-writer-benchmark")
  var records: List[GenericRecord] = _

  @Setup
  def setup(): Unit = {
    records = genPersons(size).sample.get.map(personToRecord)
  }

  @TearDown
  def shutdown(): Unit = {
    FileUtils.deleteDirectory(new File(folder))
    ()
  }

  @Benchmark
  def unsafe(): Unit = {
    val file: String = genFilePath.value()
    val writer: ParquetWriter[GenericRecord] = parquetWriter(file, conf, schema).value()
    val f = Observable
      .fromIterable(records)
      .consumeWith(ParquetSink.fromWriterUnsafe(writer))
      .runToFuture(s)
    Await.result(f, Duration.Inf)
  }

  @Benchmark
  def fromCoeval(): Unit = {
    val file: String = genFilePath.value()
    val writer: ParquetWriter[GenericRecord] = parquetWriter(file, conf, schema).value()
    val f = Observable
      .fromIterable(records)
      .consumeWith(ParquetSink.fromWriter(Coeval(writer)))
      .runToFuture(s)
    Await.result(f, Duration.Inf)

  }

}
