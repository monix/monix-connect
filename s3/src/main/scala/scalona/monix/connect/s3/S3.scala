package scalona.monix.connect.s3

import java.nio.ByteBuffer

import akka.stream.Materializer
import akka.stream.alpakka.s3.MultipartUploadResult
import monix.reactive.{Consumer, Observable, Observer}
import monix.execution.{Ack, Scheduler}
import monix.eval.Task
import scalona.monix.connect.S3RequestBuilder
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadRequest, CompleteMultipartUploadResponse, CompletedMultipartUpload, CompletedPart, CreateMultipartUploadRequest, DeleteBucketRequest, DeleteObjectRequest, DeleteObjectsRequest, EncodingType, GetObjectAclRequest, GetObjectAclResponse, GetObjectRequest, GetObjectResponse, ListObjectsResponse, ListObjectsV2Request, ListObjectsV2Response, PutObjectRequest, PutObjectResponse, RequestPayer, S3Object, UploadPartRequest, UploadPartResponse}
import akka.stream.alpakka.s3.scaladsl.{S3 => AkkaS3}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import monix.execution.cancelables.SingleAssignCancelable

import scala.concurrent.Future
import scala.jdk.FutureConverters._

object S3 {

  def getObject(bucketName: String, key: String)(implicit s3Client: S3AsyncClient): Task[ByteBuffer] = {
    val getObjectrequest = GetObjectRequest.builder().bucket(bucketName).key(key).build()
    Task.fromFuture(s3Client.getObject(getObjectrequest, new MonixS3AsyncResponseTransformer).asScala).flatten
  }

  def putObject(
    bucketName: String,
    key: String,
    content: ByteBuffer,
    contentLength: Option[Long] = None,
    contentType: Option[String] = None)(implicit s3Client: S3AsyncClient, s: Scheduler): Task[PutObjectResponse] = {
    val actualLenght: Long = contentLength.getOrElse(content.array().length.toLong)
    val putObjectRequest: PutObjectRequest =
      S3RequestBuilder.putObjectRequest(bucketName, key, actualLenght, contentType)
    val requestBody: AsyncRequestBody = AsyncRequestBody.fromPublisher(Task(content).toReactivePublisher)
    Task.fromFuture(
      s3Client.putObject(putObjectRequest, requestBody).asScala
    )
  }

  def multipartUploadConsumer(bucketName: String, key: String, contentType: Option[String] = None)(
    implicit s3Client: S3AsyncClient): Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] = {
    new MultipartUploadConsumer(bucketName, key, contentType)
  }

  def deleteObject(bucket: String, key: String)(implicit s3Client: S3AsyncClient) = {
    val deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket).key(key).build()
    Task.deferFuture(
      s3Client.deleteObject(deleteObjectRequest).asScala
    )
  }

  def deleteBucket(bucket: String, key: String)(implicit s3Client: S3AsyncClient) = {
    val deleteObjectRequest = DeleteBucketRequest.builder().bucket(bucket).build()
    Task.deferFuture(
      s3Client.deleteBucket(deleteObjectRequest).asScala
    )
  }

  def listObjects(
    bucket: String,
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None)(implicit s3Client: S3AsyncClient): Task[ListObjectsResponse] = {
    val request =
      S3RequestBuilder.listObject(bucket, delimiter, marker, maxKeys, prefix)
    Task.deferFuture(
      s3Client.listObjects(request).asScala
    )
  }

  def listObjectsV2(
    bucket: String,
    continuationToken: Option[String],
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None)(implicit s3Client: S3AsyncClient): Task[ListObjectsV2Response] = {
    val request = S3RequestBuilder.listObjectV2(
      bucket,
      continuationToken,
      delimiter,
      marker,
      maxKeys,
      prefix)
    Task.deferFuture(
      s3Client.listObjectsV2(request).asScala
    )
  }
}
