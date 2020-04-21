package cloriko.monix.connect.parquet

import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}
import org.apache.avro.generic.GenericRecord

object Parquet {

  def writer[T](writer: ParquetWriter[T])(implicit scheduler: Scheduler): Consumer[T, Task[Unit]] = {
    new ParquetSubscriber[T](writer)
  }

  def reader[T](reader: ParquetReader[T])(implicit scheduler: Scheduler): Observable[T] = {
    ParquetPublisher(reader).create
  }

}
