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

import java.io.{File, FileNotFoundException}

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.execution.schedulers.TestScheduler
import monix.reactive.Observable
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetReader
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.reflect.io.Directory
import scala.concurrent.duration._
import scala.util.Failure

class ParquetSourceSpec extends AnyWordSpecLike with Matchers with AvroParquetFixture with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }

  s"${ParquetSource}" should {

    "unsafe read from parquet reader" in {
      //given
      val n: Int = 10
      val records: List[GenericRecord] = genAvroUsers(n).sample.get.map(personToRecord)
      val file = genFilePath()
      Observable
        .fromIterable(records)
        .consumeWith(ParquetSink.fromWriterUnsafe(parquetWriter(file, conf, schema)))
        .runSyncUnsafe()

      //when
      val result: List[GenericRecord] =
        ParquetSource.fromReaderUnsafe(avroParquetReader(file, conf)).toListL.runSyncUnsafe()

      //then
      result.length shouldEqual n
      result should contain theSameElementsAs records
    }

    "safely read from parquet reader" in {
      //given
      val n: Int = 10
      val records: List[GenericRecord] = genAvroUsers(n).sample.get.map(personToRecord)
      val file = genFilePath()
      Observable
        .fromIterable(records)
        .consumeWith(ParquetSink.fromWriterUnsafe(parquetWriter(file, conf, schema)))
        .runSyncUnsafe()

      //when
      val safeReader = Task(avroParquetReader(file, conf))
      val result: List[GenericRecord] = ParquetSource.fromReader(safeReader).toListL.runSyncUnsafe()

      //then
      result.length shouldEqual n
      result should contain theSameElementsAs records
    }

    "signals failure when attempting to read from non existing file" in {
      //given
      val testScheduler = TestScheduler()
      val file = genFilePath()
      val safeReader = Task(avroParquetReader(file, conf))

      //when
      val cancelable = ParquetSource.fromReader(safeReader).toListL.runToFuture(testScheduler)

      //then
      testScheduler.tick(1.second)
      cancelable.value.get shouldBe a[Failure[FileNotFoundException]]
    }

    "signals failure when reading from a malformed reader" in {
      //given
      val testScheduler = TestScheduler()

      //when
      val malformedReader: Task[ParquetReader[GenericRecord]] = Task(null)
      val cancelable1 = ParquetSource.fromReader(malformedReader).toListL.runToFuture(testScheduler)
      val cancelable2 = ParquetSource.fromReaderUnsafe(null).toListL.runToFuture(testScheduler)

      //then
      testScheduler.tick(1.second)
      cancelable1.value.get shouldBe a[Failure[NullPointerException]]
      cancelable2.value.get shouldBe a[Failure[NullPointerException]]
    }

  }

}
