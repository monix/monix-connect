package monix.connect.gcs.utiltiies

import java.nio.ByteBuffer

import cats.effect.Resource
import com.google.cloud.WriteChannel
import com.google.cloud.storage.Storage.BlobWriteOption
import com.google.cloud.storage.{BlobInfo, Storage => GoogleStorage}
import monix.connect.gcs.utiltiies.StorageUploader.StorageConsumer
import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.Consumer
import monix.reactive.observers.{SafeSubscriber, Subscriber}

import scala.concurrent.Future
import scala.util.control.NonFatal

trait StorageUploader {

  private def openWriteChannel(storage: GoogleStorage, blobInfo: BlobInfo, chunkSize: Int, options: BlobWriteOption*): Resource[Task, WriteChannel] = {
    Resource.make {
      Task {
        val writer = storage.writer(blobInfo, options: _*)
        writer.setChunkSize(chunkSize)
        writer
      }
    } { writer =>
      Task(writer.close())
    }
  }

  def upload(storage: GoogleStorage, blobInfo: BlobInfo, chunkSize: Int, options: BlobWriteOption*): Task[StorageConsumer] = {
    openWriteChannel(storage, blobInfo, chunkSize, options: _*).use { channel =>
      Task(StorageConsumer(channel))
    }
  }
}

object StorageUploader {

  final class StorageConsumer(channel: WriteChannel) extends Consumer[Array[Byte], Unit] {
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
}