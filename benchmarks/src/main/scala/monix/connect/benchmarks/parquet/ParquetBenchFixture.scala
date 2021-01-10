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

package monix.connect.benchmarks.parquet

import java.util.UUID

import monix.eval.Coeval

trait ParquetBenchFixture {

  import org.apache.hadoop.conf.Configuration
  import org.apache.avro.Schema
  import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
  import org.apache.hadoop.fs.Path
  import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
  import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
  import org.apache.parquet.hadoop.util.HadoopInputFile
  import org.scalacheck.Gen

  case class Person(id: Int, name: String)
  val schema: Schema = new Schema.Parser().parse(
    "{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}")

  val folder: String = "./data/parquet"
  val genFilePath = Coeval(folder + "/" + UUID.randomUUID() + ".parquet")

  val conf = new Configuration()
  conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true)

  val genPerson: Gen[Person] = for {
    id   <- Gen.choose(1, 10000)
    name <- Gen.alphaLowerStr
  } yield { Person(id, name) }

  val genPersons: Int => Gen[List[Person]] = n => Gen.listOfN(n, genPerson)

  def parquetWriter(file: String, conf: Configuration, schema: Schema): Coeval[ParquetWriter[GenericRecord]] =
    Coeval(AvroParquetWriter.builder[GenericRecord](new Path(file)).withConf(conf).withSchema(schema).build())

  def avroParquetReader[T <: GenericRecord](file: String, conf: Configuration): Coeval[ParquetReader[T]] =
    Coeval(AvroParquetReader.builder[T](HadoopInputFile.fromPath(new Path(file), conf)).withConf(conf).build())

  def personToRecord(doc: Person): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("id", doc.id)
      .set("name", doc.name)
      .build()

  def recordToPerson(record: GenericRecord): Person =
    Person(
      record.get("id").asInstanceOf[Int], // unsafe only used for documentation purposes
      record.get("name").toString
    )

}
