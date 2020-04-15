package scalona.monix.connect.s3

import java.nio.ByteBuffer


import monix.reactive.{Consumer, Observable, Observer}
import monix.execution.{Ack, Scheduler}
import monix.eval.Task
import scalona.monix.connect.S3RequestBuilder
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadRequest, CompleteMultipartUploadResponse, CompletedMultipartUpload, CompletedPart, CreateMultipartUploadRequest, DeleteBucketRequest, DeleteObjectRequest, DeleteObjectsRequest, EncodingType, GetObjectAclRequest, GetObjectAclResponse, GetObjectRequest, GetObjectResponse, ListObjectsResponse, ListObjectsV2Request, ListObjectsV2Response, PutObjectRequest, PutObjectResponse, RequestPayer, S3Object, UploadPartRequest, UploadPartResponse}
import monix.execution.cancelables.SingleAssignCancelable

import scala.util.{Failure, Success, Try}

private[dynamodb] class S3(s3Client: AmazonS3) {

  def sink: Consumer[S3Object, Either[Throwable, PutObjectResult]] = {
    Consumer.create[S3Object, Either[Throwable, PutObjectResult]] { (_, _, callback) =>
      new Observer.Sync[S3Object] {
        private var putObjectResult: Either[Throwable, PutObjectResult] = _

        def onNext(s3Object: S3Object): Ack = {
          val S3Object(bucket, key, content) = s3Object
          putObjectResult = Try(s3Client.putObject(bucket, key, content)) match {
            case Success(putResult) => Right(putResult)
            case Failure(exception) => Left(exception)
          }
          monix.execution.Ack.Continue
        }

        def onComplete(): Unit = {
          callback.onSuccess(putObjectResult)
        }

        def onError(ex: Throwable): Unit = {
          callback.onError(ex)
        }
      }
    }
  }

  def getObjectAsString(bucket: String, key: String): Task[String] = {
    Task(s3Client.getObjectAsString(bucket, key))
  }
}

object S3 {
  def apply(s3Client: AmazonS3 = S3Client()): S3 = new S3(s3Client)
}
