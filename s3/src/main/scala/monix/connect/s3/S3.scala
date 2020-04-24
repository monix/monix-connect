package monix.connect.s3

import java.nio.ByteBuffer

import monix.reactive.Consumer
import monix.execution.Scheduler
import monix.eval.Task
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadResponse,
  CreateBucketResponse,
  DeleteBucketResponse,
  DeleteObjectResponse,
  GetObjectRequest,
  ListObjectsResponse,
  ListObjectsV2Response,
  PutObjectRequest,
  PutObjectResponse
}

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
    Task.deferFuture(s3Client.putObject(putObjectRequest, requestBody).asScala)
  }

  def multipartUploadConsumer(bucketName: String, key: String, contentType: Option[String] = None)(
    implicit
    s3Client: S3AsyncClient): Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] = {
    new MultipartUploadConsumer(bucketName, key, contentType)
  }

  def deleteObject(bucket: String, key: String)(implicit s3Client: S3AsyncClient): Task[DeleteObjectResponse] = {
    Task.deferFuture(s3Client.deleteObject(S3RequestBuilder.deleteObject(bucket, key)).asScala)
  }

  def deleteBucket(bucket: String)(implicit s3Client: S3AsyncClient): Task[DeleteBucketResponse] = {
    Task.deferFuture(s3Client.deleteBucket(S3RequestBuilder.deleteBucket(bucket)).asScala)
  }

  def createBucket(bucket: String)(implicit s3Client: S3AsyncClient): Task[CreateBucketResponse] = {
    Task.deferFuture(s3Client.createBucket(S3RequestBuilder.createBucket(bucket)).asScala)
  }

  def listObjects(
    bucket: String,
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None)(implicit s3Client: S3AsyncClient): Task[ListObjectsResponse] = {
    val request =
      S3RequestBuilder.listObject(bucket, delimiter, marker, maxKeys, prefix)
    Task.deferFuture(s3Client.listObjects(request).asScala)
  }

  def listObjectsV2(bucket: String)(implicit s3Client: S3AsyncClient): Task[ListObjectsV2Response] = {
    Task.deferFuture(s3Client.listObjectsV2(S3RequestBuilder.listObjectV2(bucket)).asScala)
  }

  def listObjectsV2(
    bucket: String,
    continuationToken: Option[String] = None,
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None)(implicit s3Client: S3AsyncClient): Task[ListObjectsV2Response] = {
    val request = S3RequestBuilder.listObjectV2(bucket, continuationToken, delimiter, marker, maxKeys, prefix)
    Task.deferFuture(s3Client.listObjectsV2(request).asScala)
  }

}
