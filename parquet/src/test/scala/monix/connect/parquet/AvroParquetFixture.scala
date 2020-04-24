/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package monix.connect.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalacheck.Gen

trait AvroParquetFixture extends ParquetFixture {

  case class AvroDoc(id: Int, name: String)
  val schema: Schema = new Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"AvroDoc\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}")

  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  val genAvroUser: Gen[AvroDoc] = for {
    id   <- Gen.choose(1, 10000)
    name <- Gen.alphaLowerStr
  } yield { AvroDoc(id, name) }

  val genAvroUsers: Int => Gen[List[AvroDoc]] = n => Gen.listOfN(n, genAvroUser)

  def parquetWriter(file: String, conf: Configuration, schema: Schema): ParquetWriter[GenericRecord] =
    AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build()

  def avroParquetReader[T <: GenericRecord](file: String, conf: Configuration): ParquetReader[T] =
    AvroParquetReader.builder[T](HadoopInputFile.fromPath(new Path(file), conf)).withConf(conf).build()

  def avroDocToRecord(doc: AvroDoc): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("id", doc.id)
      .set("name", doc.name)
      .build()

  def recordToAvroDoc(record: GenericRecord): AvroDoc =
    AvroDoc(record.get("id").asInstanceOf[Int], record.get("name").toString)

}
