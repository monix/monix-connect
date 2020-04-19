/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package cloriko.monix.connect.parquet

import java.io.File

import cloriko.monix.connect.parquet.tes.User.ProtoUser
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ProtoParquetSpec extends AnyWordSpecLike with Matchers with ProtoParquetFixture with AvroParquetFixture with BeforeAndAfterAll {

  s"${Parquet}" should {

    "write exactly a single protobuf message in parquet file" in {
      //given
      val n: Int = 1
      val file: String = genFile()
      val messages: List[ProtoUser] = genProtoUsers(n).sample.get
      val writer: ParquetWriter[ProtoUser] = protoParquetWriter(file)

      //when
      Observable
        .fromIterable(messages)
        .consumeWith(Parquet.writer(writer))
        .runSyncUnsafe()
        .runSyncUnsafe()

      //then
      val parquetContent: List[ProtoUser.Builder] = fromProtoParquet(file, conf)
      parquetContent.length shouldEqual n
      parquetContent.map(_.build()) should contain theSameElementsAs messages
      //parquetContent.map(_.getId()) should contain theSameElementsAs messages //this would fail if the proto parquet reader would have been instanciated as ProtoDoc
      //this tests only passes when there is only one element in the file,
      // since the proto parquet reader is broken and only will return the builder of the last element in the file
    }

    "write protobuf records in parquet (read with an avro generic record reader)" in {
      //given
      val n: Int = 4
      val file: String = genFile()
      val messages: List[ProtoUser] = genProtoUsers(n).sample.get
      val writer: ParquetWriter[ProtoUser] = protoParquetWriter(file)

      //when
      Observable
        .fromIterable(messages)
        .consumeWith(Parquet.writer(writer))
        .runSyncUnsafe()
        .runSyncUnsafe()

      //then
      val avroDocs: List[AvroDoc] = fromParquet[GenericRecord](file, conf, avroParquetReader(file, conf)).map(recordToAvroDoc)
      avroDocs.equiv(messages) shouldBe true
     }

    "read from parquet file that at most have one record" in {
      //given
      val n: Int = 1
      val records: List[ProtoUser] = genProtoUsers(n).sample.get
      println("Records: " + records.mkString(", "))

      val file = genFile()
      Observable
        .fromIterable(records)
        .consumeWith(Parquet.writer(protoParquetWriter(file)))
        .runSyncUnsafe()
        .runSyncUnsafe()

      //when
      val l: List[ProtoUser.Builder] = Parquet.reader(protoParquetReader(file, conf)).toListL.runSyncUnsafe()


      //then
      l.length shouldEqual n
      l.map(_.build()) should contain theSameElementsAs records
    }
  }

  implicit class ExtendedAvroDocList(x: List[AvroDoc]) {
    def singleEquiv(x: AvroDoc, y: ProtoUser): Boolean =
      ((x.id == y.getId) && (x.name == y.getName))
    def equiv(y: List[ProtoUser]): Boolean =
      x.zip(y).map{ case (a, p) => singleEquiv(a, p) }.filterNot(b => b).isEmpty
  }

  override def afterAll(): Unit = {
    import scala.reflect.io.Directory
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }
}
