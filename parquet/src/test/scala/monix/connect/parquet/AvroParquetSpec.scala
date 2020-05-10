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

import java.io.File

import monix.reactive.Observable
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll

class AvroParquetSpec extends AnyWordSpecLike with Matchers with AvroParquetFixture with BeforeAndAfterAll {

  s"${Parquet}" should {

    "write avro records in parquet" in {
      //given
      val n: Int = 2
      val file: String = genFile()
      val records: List[GenericRecord] = genAvroUsers(n).sample.get.map(avroDocToRecord)
      val w: ParquetWriter[GenericRecord] = parquetWriter(file, conf, schema)

      //when
      Observable
        .fromIterable(records)
        .consumeWith(Parquet.writer(w))
        .runSyncUnsafe()
        .runSyncUnsafe()

      //then
      val parquetContent: List[GenericRecord] = fromParquet[GenericRecord](file, conf, avroParquetReader(file, conf))
      parquetContent.length shouldEqual n
      parquetContent should contain theSameElementsAs records
    }

    "read from parquet file" in {
      //given
      val n: Int = 1
      val records: List[GenericRecord] = genAvroUsers(n).sample.get.map(avroDocToRecord)
      val file = genFile()
      Observable
        .fromIterable(records)
        .consumeWith(Parquet.writer(parquetWriter(file, conf, schema)))
        .runSyncUnsafe()
        .runSyncUnsafe()

      //when
      val result: List[GenericRecord] = Parquet.reader(avroParquetReader(file, conf)).toListL.runSyncUnsafe()

      //then
      result.length shouldEqual n
      result should contain theSameElementsAs records
    }
  }

  override def afterAll(): Unit = {
    import scala.reflect.io.Directory
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }
}
