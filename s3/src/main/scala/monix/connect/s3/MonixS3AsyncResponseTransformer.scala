package monix.connect.s3

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

import monix.eval.Task
import monix.reactive.Observable
import software.amazon.awssdk.core.async.{AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.model.GetObjectResponse

private[s3] class MonixS3AsyncResponseTransformer
  extends AsyncResponseTransformer[GetObjectResponse, Task[ByteBuffer]] {
  val future = new CompletableFuture[Task[ByteBuffer]]()
  override def prepare(): CompletableFuture[Task[ByteBuffer]] = future

  override def onResponse(response: GetObjectResponse): Unit = ()

  override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
    future.complete(Observable.fromReactivePublisher(publisher).headL)
    ()
  }
  override def exceptionOccurred(error: Throwable): Unit = {
    future.completeExceptionally(error)
    ()
  }
}
