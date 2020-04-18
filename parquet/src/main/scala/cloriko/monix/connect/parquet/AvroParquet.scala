package cloriko.monix.connect.parquet

import akka.actor.ActorSystem
import akka.stream.alpakka.avroparquet.scaladsl.{AvroParquetFlow, AvroParquetSource}
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import cloriko.monix.connect.akka.AkkaStreams.Implicits._
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{Consumer, Observable}
import cloriko.monix.connect.akka.AkkaStreams.sharedActorSystem

object AvroParquet {

  def writer(writer: ParquetWriter[GenericRecord])(
    implicit actorSystem: ActorSystem = sharedActorSystem,
    s: Scheduler): Consumer[GenericRecord, Task[GenericRecord]] = {
    AvroParquetFlow(writer).asConsumer
  }

  def reader(reader: ParquetReader[GenericRecord])(
    implicit actorSystem: ActorSystem = sharedActorSystem,
    scheduler: Scheduler): Observable[GenericRecord] = {
    AvroParquetSource(reader).asObservable
  }

}
