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

import monix.connect.parquet.test.User.ProtoDoc
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.proto.{ProtoParquetReader, ProtoReadSupport, ProtoWriteSupport}
import org.scalacheck.Gen

trait ProtoParquetFixture extends ParquetFixture {

  val genProtoDoc: Gen[ProtoDoc] = for {
    id   <- Gen.choose(1, 10000)
    name <- Gen.alphaLowerStr
  } yield {
    ProtoDoc.newBuilder().setId(id).setName(name).build()
  }

  val genProtoDocs: Int => Gen[List[ProtoDoc]] = n => Gen.listOfN(n, genProtoDoc)

  def protoParquetWriter(file: String): ParquetWriter[ProtoDoc] = {
    val writeSupport = new ProtoWriteSupport[ProtoDoc](classOf[ProtoDoc])
    new ParquetWriter[ProtoDoc](new Path(file), writeSupport)
  }

  /*
   * Ideally we should use this method as a parquet reader, but when the parquet is
   * read using the Protobuf schema it returns the type builder insted of the required type
   * and for when reading multiple events it just read the same event
   */
  def protoParquetReaderWithSupport(file: String, conf: Configuration): ParquetReader[ProtoDoc] = {
    val readSupport = new ProtoReadSupport[ProtoDoc]
    ParquetReader.builder[ProtoDoc](readSupport, new Path(file)).withConf(conf).build()
  }

  /*
   * A parquet reader with no reader support passed would create
   *  a generic non typed parquet reader support instance under the hood
   */
  def protoParquetReader(file: String, conf: Configuration): ParquetReader[ProtoDoc.Builder] = {
    ProtoParquetReader.builder[ProtoDoc.Builder](new Path(file)).withConf(conf).build()
  }

  /*
   * A parquet reader with no reader support passed would create
   *  a generic non typed parquet reader support instance under the hood
   */
  def protoParquetReaderT(file: String, conf: Configuration): ParquetReader[ProtoDoc] = {
    ProtoParquetReader.builder[ProtoDoc](new Path(file)).withConf(conf).build()
  }

  def fromProtoParquet(file: String, configuration: Configuration): List[ProtoDoc.Builder] = {
    val reader: ParquetReader[ProtoDoc.Builder] = protoParquetReader(file, conf)
    var proto: ProtoDoc.Builder = reader.read()
    var result: List[ProtoDoc.Builder] = List.empty[ProtoDoc.Builder]
    while (proto != null) {
      result = result ::: proto :: Nil
      proto = reader.read()
    }
    result
  }

}
