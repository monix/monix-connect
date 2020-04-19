/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package cloriko.monix.connect.parquet

import cloriko.monix.connect.parquet.tes.User.ProtoUser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ ParquetReader, ParquetWriter }
import org.apache.parquet.proto.{ ProtoParquetReader, ProtoReadSupport, ProtoWriteSupport }
import org.scalacheck.Gen

trait ProtoParquetFixture extends ParquetFixture {

  val genProtoUser: Gen[ProtoUser] = for {
    id   <- Gen.choose(1, 10000)
    name <- Gen.alphaLowerStr
  } yield {
    ProtoUser.newBuilder().setId(id).setName(name).build()
  }

  val genProtoUsers: Int => Gen[List[ProtoUser]] = n => Gen.listOfN(n, genProtoUser)

  def protoParquetWriter(file: String): ParquetWriter[ProtoUser] = {
    val writeSupport = new ProtoWriteSupport[ProtoUser](classOf[ProtoUser])
    new ParquetWriter[ProtoUser](new Path(file), writeSupport)
  }

  /*
   * Ideally we should use this method as a parquet reader, but when the parquet is
   * read using the Protobuf schema it returns the type builder insted of the required type
   * and for when reading multiple events it just read the same event
   */
  def protoParquetReaderWithSupport(file: String, conf: Configuration): ParquetReader[ProtoUser] = {
    val readSupport = new ProtoReadSupport[ProtoUser]
    ParquetReader.builder[ProtoUser](readSupport, new Path(file)).withConf(conf).build()
  }

  /*
   * A parquet reader with no reader support passed would create
   *  a generic non typed parquet reader support instance under the hood
   */
  def protoParquetReader(file: String, conf: Configuration): ParquetReader[ProtoUser.Builder] = {
    ProtoParquetReader.builder[ProtoUser.Builder](new Path(file)).withConf(conf).build()
  }

  def fromProtoParquet(file: String, configuration: Configuration): List[ProtoUser.Builder] = {
    val reader: ParquetReader[ProtoUser.Builder] = protoParquetReader(file, conf)
    var proto: ProtoUser.Builder = reader.read()
    var result: List[ProtoUser.Builder] = List.empty[ProtoUser.Builder]
    while (proto != null) {
      result = result ::: proto :: Nil
      proto = reader.read()
    }
    result
  }

}
