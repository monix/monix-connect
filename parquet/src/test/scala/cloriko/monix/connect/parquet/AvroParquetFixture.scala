/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package cloriko.monix.connect.parquet

import cloriko.monix.connect.parquet.tes.User.ProtoUser
import org.apache.hadoop.conf.Configuration
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.proto.ProtoParquetReader
import org.scalacheck.Gen

trait AvroParquetFixture extends ParquetFixture {

  case class UserInfo(id: String, name: String)
  val schema: Schema = new Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"UserInfo\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}")

  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  val genAvroUser: Gen[UserInfo] =
    Gen.oneOf(Seq(UserInfo(id = Gen.alphaStr.sample.get, name = Gen.alphaLowerStr.sample.get)))
  val genAvroUsers: Int => Gen[List[UserInfo]] = n => Gen.listOfN(n, genAvroUser)

  def parquetWriter(file: String, conf: Configuration, schema: Schema): ParquetWriter[GenericRecord] =
    AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build()

  def avroParquetReader[T <: GenericRecord](file: String, conf: Configuration): ParquetReader[T] =
    AvroParquetReader.builder[T](HadoopInputFile.fromPath(new Path(file), conf)).withConf(conf).build()

  def userInfoToRecord(userInfo: UserInfo): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("id", userInfo.id)
      .set("name", userInfo.name)
      .build()


}
