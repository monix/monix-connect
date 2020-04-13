package scalona.monix.connect.s3

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

import monix.reactive.{ Consumer, Observable, Observer }
import monix.execution.{ Ack, Scheduler }
import monix.eval.Task
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{ CompleteMultipartUploadRequest, CompleteMultipartUploadResponse, CompletedMultipartUpload, CompletedPart, CreateMultipartUploadRequest, GetObjectAclRequest, GetObjectAclResponse, GetObjectRequest, GetObjectResponse, PutObjectRequest, PutObjectResponse, S3Object, UploadPartRequest, UploadPartResponse }

import scala.jdk.FutureConverters._
import scala.jdk.CollectionConverters._

object S3 {

  def putObject(
    bucketName: String,
    key: String,
    content: ByteBuffer,
    contentLength: Option[Long] = None,
    contentType: Option[String] = None)(implicit s3Client: S3AsyncClient, s: Scheduler): Task[PutObjectResponse] = {
    val contentLenght: Long = contentLength.getOrElse(content.array().length.toLong)
    val putObjectRequest = PutObjectRequest
      .builder()
      .bucket(bucketName)
      .contentLength(contentLenght)
      .contentType(contentType.getOrElse("plain/text"))
      .key(key)
      .build()
    val requestBody = AsyncRequestBody.fromPublisher(Task(content).toReactivePublisher)
    Task.fromFuture(
      s3Client.putObject(putObjectRequest, requestBody).asScala
    )
  }
  val awsPartSizeLimit = 5 * 1024 * 1024

  def divideChunk(byteBuffer: List[Byte]): List[List[Byte]] = {
    val bufferSize = byteBuffer.size
    if (bufferSize > awsPartSizeLimit) {
      val (l1: List[Byte], l2: List[Byte]) = byteBuffer.splitAt(bufferSize / 2)
      (divideChunk(l1) :: divideChunk(l2) :: Nil).flatten
    } else List(byteBuffer)
  }

  def multipartUpload(
    bucketName: String,
    key: String,
    contentStream: Observable[ByteBuffer],
    contentType: Option[String] = None)(
    implicit s3Client: S3AsyncClient,
    s: Scheduler): Task[CompleteMultipartUploadResponse] = {
    val multiPartUploadrequest: CreateMultipartUploadRequest = CreateMultipartUploadRequest
      .builder()
      .bucket(bucketName)
      .contentType(contentType.getOrElse("plain/text"))
      .key(key)
      .build()

    val futureUploadId: Task[String] =
      Task.fromFuture(s3Client.createMultipartUpload(multiPartUploadrequest).asScala.map(_.uploadId()))

    futureUploadId.flatMap { uploadId =>
      val t: Task[List[CompletedPart]] = contentStream
        .flatMap(buffer =>
          Observable.fromIterable[ByteBuffer](divideChunk(buffer.array().toList).map(bytesList =>
            ByteBuffer.wrap(bytesList.toArray))))
        .mapAccumulate[Int, (Int, ByteBuffer)](0)((acc, bytes) => (acc + 1, (acc, bytes)))
        .map {
          case (partNumber, byteBuffer: ByteBuffer) =>
            val uploadPartRequest = UploadPartRequest
              .builder()
              .bucket(bucketName)
              .key(key)
              .partNumber(partNumber)
              .uploadId(uploadId)
              .contentLength(byteBuffer.array().length.toLong)
              .build()
            val asyncRequestBody = AsyncRequestBody.fromBytes(byteBuffer.array())
            Task
              .fromFuture(
                s3Client
                  .uploadPart(
                    uploadPartRequest,
                    asyncRequestBody
                  )
                  .asScala)
              .map(resp => CompletedPart.builder().partNumber(partNumber).eTag(resp.eTag()).build())
        }
        .toListL
        .flatMap(Task.sequence(_))

      val completeMultipartUploadResp: Task[CompleteMultipartUploadResponse] = t.flatMap { completedParts =>
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

      completeMultipartUploadResp
    }

  }

  def multipartUploadConsumer(
    bucketName: String,
    key: String,
    contentType: Option[String] = None)(
    implicit s3Client: S3AsyncClient): Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] = {
    Consumer.create[Array[Byte], Task[CompleteMultipartUploadResponse]] { (scheduler, _, callback) =>
    implicit val s = scheduler
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


      new Observer.Sync[Array[Byte]] {
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
          val completeMultipartUploadResp: Task[CompleteMultipartUploadResponse] = Task.sequence(completedParts).flatMap { completedParts =>
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

        def onError(ex: Throwable): Unit = {
          callback.onError(ex)
        }
      }
    }
  }

  def getObject(bucketName: String, key: String)(implicit s3Client: S3AsyncClient): Task[ByteBuffer] = {
    val getObjectrequest = GetObjectRequest.builder().bucket(bucketName).key(key).build()
    Task.fromFuture(s3Client.getObject(getObjectrequest, new MonixS3AsyncResponseTransformer).asScala).flatten
  }

}
