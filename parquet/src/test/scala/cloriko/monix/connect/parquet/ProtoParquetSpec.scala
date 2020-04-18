/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package cloriko.monix.connect.parquet

import java.io.File

import cloriko.monix.connect.parquet.tes.User.ProtoUser
import cloriko.monix.connect.parquet.tes.user.{ProtoUser => ScalaProtoUser}
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.proto.{ProtoParquetWriter, ProtoWriteSupport}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ProtoParquetSpec extends AnyWordSpecLike with Matchers with ProtoParquetFixture with BeforeAndAfterAll {

  s"${Parquet}" should {

    "write protobuf records in parquet" in {
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
      val parquetContent: List[ProtoUser] = fromProtoParquet(file, conf)
      parquetContent.length shouldEqual n
      parquetContent should contain theSameElementsAs messages
    }

  }


  override def afterAll(): Unit = {
    import scala.reflect.io.Directory
    val directory = new Directory(new File(folder))
    directory.deleteRecursively()
  }
}
