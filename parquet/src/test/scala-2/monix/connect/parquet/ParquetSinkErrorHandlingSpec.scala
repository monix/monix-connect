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

import monix.eval.Coeval
import monix.execution.Scheduler
import monix.execution.exceptions.DummyException
import monix.execution.schedulers.TestScheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskTest
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.mockito.IdiomaticMockito
import org.mockito.MockitoSugar.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File

class ParquetSinkErrorHandlingSpec
  extends AsyncWordSpec with MonixTaskTest with IdiomaticMockito with Matchers with AvroParquetFixture
  with BeforeAndAfterAll {

  override implicit val scheduler: Scheduler = Scheduler.io("parquet-sink-coeval-spec")

  override def afterAll(): Unit = {
    import scala.reflect.io.Directory
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }

  s"$ParquetSink" should {

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

    "signals error when the underlying parquet writer throws an error" in {
      val testScheduler = TestScheduler()
      val filePath: String = genFilePath()
      val record: GenericRecord = personToRecord(genPerson.sample.get)
      val ex = DummyException("Boom!")
      val parquetWriter = mock[ParquetWriter[GenericRecord]]
      when(parquetWriter.write(record)).thenThrow(ex)

      Observable
        .now(record)
        .consumeWith(ParquetSink.fromWriter(Coeval(parquetWriter)))
        .attempt
        .asserting { attempt =>
          attempt.isLeft shouldBe true
          val file = new File(filePath)
          file.exists() shouldBe false
        }
    }

  }

}
