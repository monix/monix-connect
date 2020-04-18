/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package cloriko.monix.connect.parquet

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalacheck.Gen

trait AvroParquetFixture {

  case class UserInfo(id: String, name: String)
  val schema: Schema = new Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"UserInfo\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\"}]}"
  )

  val genUserInfo: Gen[UserInfo] =
    Gen.oneOf(Seq(UserInfo(id = Gen.alphaStr.sample.get, name = Gen.alphaLowerStr.sample.get)))
  val genUsersInfo: Int => Gen[List[UserInfo]] = n => Gen.listOfN(n, genUserInfo)

  val folder: String = "./" + Gen.alphaLowerStr.sample.get
  val genFile: () => String = () => folder + "/" + Gen.alphaLowerStr.sample.get + ".parquet"

  val conf = new Configuration()
  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  def parquetWriter(file: String, conf: Configuration, schema: Schema): ParquetWriter[GenericRecord] =
    AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build()

  def parquetReader[T <: GenericRecord](file: String, conf: Configuration): ParquetReader[T] =
    AvroParquetReader.builder[T](HadoopInputFile.fromPath(new Path(file), conf)).withConf(conf).build()

  def userInfoToRecord(userInfo: UserInfo): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("id", userInfo.id)
      .set("name", userInfo.name)
      .build()

  def fromParquet[T <: GenericRecord](file: String, configuration: Configuration): List[T] = {
    val reader = parquetReader(file, conf)
    var record: T = reader.read()
    var result: List[T] = List.empty[T]
    while (record != null) {
      result = result ::: record :: Nil
      record = reader.read()
    }
    result
  }

}
