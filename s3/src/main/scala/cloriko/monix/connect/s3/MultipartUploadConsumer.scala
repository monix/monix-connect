package cloriko.monix.connect.s3

import monix.eval.Task
import monix.execution.{ Ack, Callback, Scheduler }
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{ CompleteMultipartUploadResponse, CompletedPart, CreateMultipartUploadRequest, UploadPartRequest }

import scala.jdk.FutureConverters._

private[s3] class MultipartUploadConsumer(bucket: String, key: String, contentType: Option[String] = None)(
  implicit s3Client: S3AsyncClient)
  extends Consumer.Sync[Array[Byte], Task[CompleteMultipartUploadResponse]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[CompleteMultipartUploadResponse]],
    s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val out = new Subscriber.Sync[Array[Byte]] {
      implicit val scheduler = s
      private[this] var hasMinSize = false //todo contemplate if chunks comply with minimum size of 5MB
      private[this] val partN = 0
      private[this] var completedParts: List[Task[CompletedPart]] = List()
      private[this] val multiPartUploadrequest: CreateMultipartUploadRequest =
        S3RequestBuilder.multipartUploadRequest(bucket, key, contentType)
      private[this] val uploadId: String =
        Task
          .fromFuture(
            s3Client
              .createMultipartUpload(multiPartUploadrequest)
              .asScala
              .map(_.uploadId())
          )
          .runSyncUnsafe()

      def onNext(chunk: Array[Byte]): Ack = {
        val uploadPartReq: UploadPartRequest =
          S3RequestBuilder.uploadPartRequest(bucket, key, partN, uploadId, chunk.size.toLong)
        val asyncRequestBody = AsyncRequestBody.fromBytes(chunk)
        val completedPart: Task[CompletedPart] = Task
          .fromFuture(
            s3Client
              .uploadPart(
                uploadPartReq,
                asyncRequestBody
              )
              .asScala)
          .map(resp => S3RequestBuilder.completedPart(partN, resp))
        completedParts = completedPart :: completedParts
        monix.execution.Ack.Continue
      }

      def onComplete(): Unit = {
        val completeMultipartUploadResp: Task[CompleteMultipartUploadResponse] = Task.sequence(completedParts).flatMap {
          completedParts =>
            val completeMultipartUploadrequest = S3RequestBuilder
              .completeMultipartUploadRquest(bucket, key, uploadId, completedParts)
            Task.fromFuture(
              s3Client
                .completeMultipartUpload(
                  completeMultipartUploadrequest
                )
                .asScala
            )
        }
        callback.onSuccess(completeMultipartUploadResp)
      }

      def onError(ex: Throwable): Unit =
        callback.onError(ex)
    }

    (out, AssignableCancelable.dummy)
  }
}
