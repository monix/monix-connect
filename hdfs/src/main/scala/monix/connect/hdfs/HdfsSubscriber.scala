package monix.connect.hdfs

import monix.eval.Task
import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsSubscriber(fs: FileSystem, path: Path) extends Consumer.Sync[Array[Byte], Task[Int]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[Int]],
    s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val sub = new Subscriber.Sync[Array[Byte]] {
      val out = fs.create(path)
      var off = 0

      override implicit def scheduler: Scheduler = s

      override def onComplete() = {
        out.close()
        callback.onSuccess(Task(off))
      }

      override def onError(ex: Throwable): Unit = {
        callback.onError(ex)
      }

      override def onNext(elem: Array[Byte]): Ack = {
        val len = elem.length
        out.write(elem, off, len)
        off += len
        Ack.Continue
      }
    }

    (sub, AssignableCancelable.dummy)
  }

}
