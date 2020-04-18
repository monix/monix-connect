/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package cloriko.monix.connect.parquet

import cloriko.monix.connect.parquet.tes.User.ProtoUser
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.scalacheck.Gen

trait ParquetFixture {

  val folder: String = "./" + Gen.alphaLowerStr.sample.get
  val genFile: () => String = () => folder + "/" + Gen.alphaLowerStr.sample.get + ".parquet"

  val conf = new Configuration()

  def fromParquet[T](file: String, configuration: Configuration, reader: ParquetReader[T]): List[T] = {

    var record: T = reader.read()

    var result: List[T] = List.empty[T]
    while (record != null) {
      result = result ::: record :: Nil
      record = reader.read()
    }
    result
  }

}
