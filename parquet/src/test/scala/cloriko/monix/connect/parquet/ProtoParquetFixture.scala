/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package cloriko.monix.connect.parquet

import cloriko.monix.connect.parquet.tes.User.ProtoUser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.proto.{ProtoParquetReader, ProtoWriteSupport}
import org.scalacheck.Gen
trait ProtoParquetFixture extends ParquetFixture {

  val genProtoUser: Gen[ProtoUser] =
    Gen.oneOf(Seq(ProtoUser.newBuilder().setId(Gen.posNum[Int].sample.get).setName(Gen.alphaStr.sample.get).build()))
  val genProtoUsers: Int => Gen[List[ProtoUser]] = n => Gen.listOfN(n, genProtoUser)

  def protoParquetWriter(file: String): ParquetWriter[ProtoUser] = {
    val writeSupport = new ProtoWriteSupport[ProtoUser](classOf[ProtoUser])
    new ParquetWriter[ProtoUser](new Path(file), writeSupport)
  }

  def protoParquetReader[T](file: String, conf: Configuration): ParquetReader[T] =
    ProtoParquetReader.builder[T](new Path(file)).withConf(conf).build()

  def fromProtoParquet(file: String, configuration: Configuration): List[ProtoUser] = {
    val reader: ParquetReader[ProtoUser.Builder] = protoParquetReader(file, conf)
    var proto: ProtoUser.Builder = reader.read()
    var result: List[ProtoUser.Builder] = List.empty[ProtoUser.Builder]
    while (proto != null) {
      result = result ::: proto :: Nil
      proto = reader.read()
    }
    result.map(_.build())
  }

}
