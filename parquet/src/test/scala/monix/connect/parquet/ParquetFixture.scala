/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package monix.connect.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetReader
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
