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

import monix.connect.parquet.test.User.ProtoDoc
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

@Ignore //"There is a blocker on writing from proto to parquet: https://github.com/scalapb/ScalaPB/issues/844" 
class ProtoParquetSpec
  extends AnyWordSpecLike with Matchers with ProtoParquetFixture with AvroParquetFixture with BeforeAndAfterAll
  with Eventually {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  s"${Parquet}" should {

    "write exactly a single protobuf message in parquet file" in {
      //given
      val file: String = genFilePath()
      val messages: ProtoDoc = genProtoDoc.sample.get
      val writer: ParquetWriter[ProtoDoc] = protoParquetWriter(file)

      //when
      Observable
        .pure(messages)
        .consumeWith(Parquet.writer(writer))
        .runSyncUnsafe()

      //then
      val parquetContent: List[ProtoDoc.Builder] = fromProtoParquet(file, conf)
      parquetContent.length shouldEqual 1
      parquetContent.map(_.build()) should contain theSameElementsAs List(messages)
      //parquetContent.map(_.getId()) should contain theSameElementsAs messages //this would fail if the proto parquet reader would have been instanciated as ProtoDoc
      //this tests only passes when there is only one element in the file,
      // since the proto parquet reader is broken and only will return the builder of the last element in the file
    }

    "write protobuf records in parquet (read with an avro generic record reader)" in {
      //given
      val n: Int = 2
      val file: String = genFilePath()
      val messages: List[ProtoDoc] = genProtoDocs(n).sample.get
      val writer: ParquetWriter[ProtoDoc] = protoParquetWriter(file)

      //when
      Observable
        .fromIterable(messages)
        .consumeWith(Parquet.writer(writer))
        .runSyncUnsafe()

      //then
      eventually {
        val avroDocs: List[AvroDoc] =
          fromParquet[GenericRecord](file, conf, avroParquetReader(file, conf)).map(recordToAvroDoc)
        assert(avroDocs.equiv(messages))
      }
    }

    "read from parquet file that at most have one record" in {
      //given
      val records: ProtoDoc = genProtoDoc.sample.get
      val file: String = genFilePath()
      Observable
        .pure(records)
        .consumeWith(Parquet.writer(protoParquetWriter(file)))
        .runSyncUnsafe()

      //when
      val l: List[ProtoDoc.Builder] = Parquet.reader(protoParquetReader(file, conf)).toListL.runSyncUnsafe()

      //then
      l.length shouldEqual 1
      l.map(_.build()) should contain theSameElementsAs List(records)
    }

  }

  override def afterAll(): Unit = {
    import scala.reflect.io.Directory
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }
}
