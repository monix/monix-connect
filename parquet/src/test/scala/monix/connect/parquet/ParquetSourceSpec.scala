/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
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

import java.io.{File, FileNotFoundException}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskTest
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.reflect.io.Directory

class ParquetSourceSpec
  extends AsyncWordSpec with MonixTaskTest with Matchers with AvroParquetFixture with BeforeAndAfterAll {

  override implicit val scheduler: Scheduler = Scheduler.io("parquet-source-spec")
  override def afterAll(): Unit = {
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }

  s"$ParquetSource" should {

    "unsafe read from parquet reader" in {
      val n: Int = 10
      val records: List[GenericRecord] = genAvroUsers(n).sample.get.map(personToRecord)
      val file = genFilePath()

      for {
        _ <- Observable
          .fromIterable(records)
          .consumeWith(ParquetSink.fromWriterUnsafe(parquetWriter(file, conf, schema)))
        result <- ParquetSource.fromReaderUnsafe(avroParquetReader(file, conf)).toListL: Task[List[GenericRecord]]
      } yield {
        result.length shouldEqual n
        result should contain theSameElementsAs records
      }
    }

    "safely read from parquet reader" in {
      val n: Int = 10
      val records: List[GenericRecord] = genAvroUsers(n).sample.get.map(personToRecord)
      val file = genFilePath()

      for {
        _ <- Observable
          .fromIterable(records)
          .consumeWith(ParquetSink.fromWriterUnsafe(parquetWriter(file, conf, schema)))
        safeReader = Task(avroParquetReader(file, conf))
        result <- ParquetSource.fromReader(safeReader).toListL: Task[List[GenericRecord]]
      } yield {
        result.length shouldEqual n
        result should contain theSameElementsAs records
      }
    }

    "signals failure when attempting to read from non existing file" in {
      val file = genFilePath()
      val safeReader = Task(avroParquetReader(file, conf))

      ParquetSource.fromReader(safeReader).toListL.assertThrows[FileNotFoundException]
    }

    "signals failure when reading from a malformed reader" in {
      val malformedReader: Task[ParquetReader[GenericRecord]] = Task(null)
      for {
        cancelable1 <- ParquetSource.fromReader(malformedReader).toListL.attempt
        cancelable2 <- ParquetSource.fromReaderUnsafe(null).toListL.attempt
      } yield {
        cancelable1.isLeft shouldBe true
        cancelable2.isLeft shouldBe true
        cancelable1.left.get shouldBe a[NullPointerException]
      }
    }

  }

}
