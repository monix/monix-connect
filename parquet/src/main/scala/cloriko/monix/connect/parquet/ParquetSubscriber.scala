package cloriko.monix.connect.parquet

import monix.eval.Task
import monix.execution.{ Ack, Callback, Scheduler }
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter

class ParquetSubscriber[T](parquetWriter: ParquetWriter[T]) extends Consumer.Sync[T, Task[Unit]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[Unit]],
    s: Scheduler): (Subscriber.Sync[T], AssignableCancelable) = {
    val out = new Subscriber.Sync[T] {
      override implicit def scheduler: Scheduler = s

      override def onComplete() = {
        parquetWriter.close()
        callback.onSuccess(Task())
      }

      override def onError(ex: Throwable): Unit = {
        parquetWriter.close()
        callback.onError(ex)
      }

      override def onNext(record: T): Ack = {
        parquetWriter.write(record)
        monix.execution.Ack.Continue
      }
    }

    (out, AssignableCancelable.dummy)
  }

}
