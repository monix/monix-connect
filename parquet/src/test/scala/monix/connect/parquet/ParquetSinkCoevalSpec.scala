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

import java.io.File
import monix.eval.Coeval
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskTest
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class ParquetSinkCoevalSpec
  extends AsyncWordSpec with MonixTaskTest with Matchers with AvroParquetFixture with BeforeAndAfterAll {

  override implicit val scheduler: Scheduler = Scheduler.io("parquet-sink-coeval-spec")

  override def afterAll(): Unit = {
    deleteRecursively(new File(folder))
  }

  s"$ParquetSink" should {

    "unsafe write avro records in parquet" in {
      val n: Int = 2
      val filePath: String = genFilePath()
      val records: List[GenericRecord] = genAvroUsers(n).sample.get.map(personToRecord)
      val w: ParquetWriter[GenericRecord] = parquetWriter(filePath, conf, schema)

      Observable
        .fromIterable(records)
        .consumeWith(ParquetSink.fromWriter(Coeval(w)))
        .asserting { _ =>
          val parquetContent: List[GenericRecord] =
            fromParquet[GenericRecord](filePath, conf, avroParquetReader(filePath, conf))
          parquetContent.length shouldEqual n
          parquetContent should contain theSameElementsAs records
        }
    }

    "materialises to 0 when an empty observable is passed" in {
      val filePath: String = genFilePath()
      val w: ParquetWriter[GenericRecord] = parquetWriter(filePath, conf, schema)

      Observable
        .empty[GenericRecord]
        .consumeWith(ParquetSink.fromWriter(Coeval(w)))
        .asserting { writtenRecords =>
          writtenRecords shouldBe 0
          val file = new File(filePath)
          val parquetContent: List[GenericRecord] =
            fromParquet[GenericRecord](filePath, conf, avroParquetReader(filePath, conf))
          file.exists() shouldBe true
          parquetContent.length shouldEqual 0
        }
    }

    "signals error when a malformed parquet writer was passed" in {
      val n = 1
      val filePath: String = genFilePath()
      val records: List[GenericRecord] = genAvroUsers(n).sample.get.map(personToRecord)

      Observable
        .fromIterable(records)
        .consumeWith(ParquetSink.fromWriter(Coeval(null)))
        .attempt
        .asserting { attempt =>
          attempt.isLeft shouldBe true

          val file = new File(filePath)
          file.exists() shouldBe false
        }
    }

  }

}
