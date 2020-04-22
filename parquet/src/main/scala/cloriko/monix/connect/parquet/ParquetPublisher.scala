package cloriko.monix.connect.parquet

import monix.eval.Task
import monix.reactive.{ Observable, OverflowStrategy }
import monix.reactive.observers.Subscriber
import org.apache.parquet.hadoop.ParquetReader

import monix.execution.Ack
private[parquet] class ParquetPublisher[T](reader: ParquetReader[T]) {

  private def readRecords(sub: Subscriber[T]): Task[Unit] = {
    val t = Task(reader.read())
    t.flatMap { r =>
      if (r != null) {
        Task.deferFuture(sub.onNext(r)).flatMap {
          case Ack.Continue => readRecords(sub)
          case Ack.Stop => Task.unit
        }
      } else {
        sub.onComplete()
        Task.unit
      }
    }
  }

  val create: Observable[T] =
    Observable.create(OverflowStrategy.Unbounded) { sub => readRecords(sub).runToFuture(sub.scheduler) }
}

object ParquetPublisher {
  def apply[T](reader: ParquetReader[T]): ParquetPublisher[T] = new ParquetPublisher(reader)
}
