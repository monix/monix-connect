package monix.connect.gcs.components

import java.nio.ByteBuffer

import com.google.cloud.WriteChannel
import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.{SafeSubscriber, Subscriber}

import scala.concurrent.Future
import scala.util.control.NonFatal

private[gcs] final class StorageConsumer(channel: WriteChannel) extends Consumer[Array[Byte], Unit] {
  override def createSubscriber(cb: Callback[Throwable, Unit], s: Scheduler): (Subscriber[Array[Byte]], AssignableCancelable) = {
    val out = SafeSubscriber(new Subscriber[Array[Byte]] { self =>

      override implicit def scheduler: Scheduler = s

      override def onNext(elem: Array[Byte]): Future[Ack] =
        Future(channel.write(ByteBuffer.wrap(elem)))
          .map(_ => Ack.Continue)
          .recover {
            case NonFatal(_) => Ack.Stop
          }

      override def onError(ex: Throwable): Unit =
        cb.onError(ex)

      override def onComplete(): Unit =
        cb.onSuccess(())
    })

    (out, AssignableCancelable.dummy)
  }
}

object StorageConsumer {
  def apply(channel: WriteChannel): StorageConsumer = new StorageConsumer(channel)
}