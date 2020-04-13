package scalona.monix.connect.s3

import monix.eval.Task
import monix.execution.{ Ack, Callback, Scheduler }
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{ CompleteMultipartUploadRequest, CompleteMultipartUploadResponse, CompletedMultipartUpload, CompletedPart, CreateMultipartUploadRequest, UploadPartRequest }

import scala.jdk.FutureConverters._
import scala.jdk.CollectionConverters._

private[s3] class MultipartUploadConsumer(bucketName: String, key: String, contentType: Option[String] = None)(
  implicit s3Client: S3AsyncClient)
  extends Consumer.Sync[Array[Byte], Task[CompleteMultipartUploadResponse]] {

  def createSubscriber(
    callback: Callback[Throwable, Task[CompleteMultipartUploadResponse]],
    s: Scheduler): (Subscriber.Sync[Array[Byte]], AssignableCancelable) = {
    val out = new Subscriber.Sync[Array[Byte]] {
      implicit val scheduler = s
      private[this] var isDone = false
      val partCounter = 0
      var completedParts: List[Task[CompletedPart]] = List()
      val multiPartUploadrequest: CreateMultipartUploadRequest = CreateMultipartUploadRequest
        .builder()
        .bucket(bucketName)
        .contentType(contentType.getOrElse("plain/text"))
        .key(key)
        .build()
      val uploadId: String =
        Task
          .fromFuture(
            s3Client
              .createMultipartUpload(multiPartUploadrequest)
              .asScala
              .map(_.uploadId())
          )
          .runSyncUnsafe()

      def onNext(chunk: Array[Byte]): Ack = {
        val uploadPartRequest = UploadPartRequest
          .builder()
          .bucket(bucketName)
          .key(key)
          .partNumber(partCounter)
          .uploadId(uploadId)
          .contentLength(chunk.size.toLong)
          .build()
        val asyncRequestBody = AsyncRequestBody.fromBytes(chunk)
        val completedPart: Task[CompletedPart] = Task
          .fromFuture(
            s3Client
              .uploadPart(
                uploadPartRequest,
                asyncRequestBody
              )
              .asScala)
          .map(resp => CompletedPart.builder().partNumber(partCounter).eTag(resp.eTag()).build())
        completedParts = completedPart :: completedParts
        monix.execution.Ack.Continue
      }

      def onComplete(): Unit = {
        val completeMultipartUploadResp: Task[CompleteMultipartUploadResponse] = Task.sequence(completedParts).flatMap {
          completedParts =>
            val completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts.asJava).build()
            Task.fromFuture(
              s3Client
                .completeMultipartUpload(
                  CompleteMultipartUploadRequest
                    .builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build()
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
