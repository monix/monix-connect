package cloriko.monix.connect.parquet

import org.apache.parquet.hadoop.{ ParquetReader, ParquetWriter }
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{ Consumer, Observable }
import org.apache.avro.generic.GenericRecord

object Parquet {

  def writer[T <: GenericRecord](writer: ParquetWriter[T])(implicit scheduler: Scheduler): Consumer[T, Task[Unit]] = {
    new ParquetSubscriber[T](writer)
  }

  def reader[T <: GenericRecord](reader: ParquetReader[T])(implicit scheduler: Scheduler): Observable[T] = {
    ParquetPublisher(reader).create
  }

}
