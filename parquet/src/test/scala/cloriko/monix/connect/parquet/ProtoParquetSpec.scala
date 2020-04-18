/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package cloriko.monix.connect.parquet

import java.io.File

import cloriko.monix.connect.parquet.tes.User.ProtoUser
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.proto.{ProtoParquetWriter, ProtoWriteSupport}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class ProtoParquetSpec extends AnyWordSpecLike with Matchers with AvroParquetFixture with BeforeAndAfterAll {

  s"${Parquet}" should {

    "write avro records in parquet" in {
      //given
      val n: Int = 2
      val file: String = "testParquet"
      val records: List[GenericRecord] = genUsersInfo(n).sample.get.map(userInfoToRecord)

      val support = new ProtoWriteSupport[ProtoUser](classOf[ProtoUser])

      val protoWriter = new ParquetWriter[ProtoUser](new Path(file), support)

      protoWriter.write(ProtoUser.newBuilder().setId(1).setName("").build())
      protoWriter.close()
      //when

    }

    "read from parquet file" in {
      //given
      val n: Int = 4
      val records: List[GenericRecord] = genUsersInfo(n).sample.get.map(userInfoToRecord)
      val file = genFile()

      Observable
        .fromIterable(records)
        .consumeWith(Parquet.writer(parquetWriter(file, conf, schema)))
        .runSyncUnsafe()
        .runSyncUnsafe()

      //when
      val result: List[GenericRecord] = Parquet.reader(parquetReader(file, conf)).toListL.runSyncUnsafe()
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
