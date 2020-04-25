/*
 * Copyright (c) 2014-2020 by The Monix Project Developers.
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
