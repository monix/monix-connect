package cloriko.monix.connect.s3

import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart, CreateBucketRequest, CreateMultipartUploadRequest, DeleteBucketRequest, DeleteObjectRequest, EncodingType, ListObjectsRequest, ListObjectsV2Request, PutObjectRequest, RequestPayer, UploadPartRequest, UploadPartResponse}

import scala.jdk.CollectionConverters._

object S3RequestBuilder {

  def putObjectRequest(bucket: String, key: String, contentLenght: Long, contentType: Option[String]) =
    PutObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .contentLength(contentLenght)
      .contentType(contentType.getOrElse("plain/text"))
      .build()

  def uploadPartRequest(bucketName: String, key: String, partN: Int, uploadId: String, contentLenght: Long) =
    UploadPartRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .partNumber(partN)
      .uploadId(uploadId)
      .contentLength(contentLenght)
      .build()

  def completedPart(partN: Int, uploadPartResp: UploadPartResponse) =
    CompletedPart.builder().partNumber(partN).eTag(uploadPartResp.eTag()).build()

  def multipartUploadRequest(bucketName: String, key: String, contentType: Option[String]) =
    CreateMultipartUploadRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .contentType(contentType.getOrElse("plain/text"))
      .build()

  def completeMultipartUploadRquest(
    bucket: String,
    key: String,
    uploadId: String,
    completedParts: List[CompletedPart]): CompleteMultipartUploadRequest = {
    val completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts.asJava).build()
    CompleteMultipartUploadRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .uploadId(uploadId)
      .multipartUpload(completedMultipartUpload)
      .build()
  }

  def listObjectV2(
    bucket: String,
    continuationToken: Option[String] = None,
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None) = {
    val listObjectsBuilder = ListObjectsV2Request.builder().bucket(bucket)
    continuationToken.map(listObjectsBuilder.continuationToken(_))
    delimiter.map(listObjectsBuilder.delimiter(_))
    maxKeys.map(listObjectsBuilder.maxKeys(_))
    prefix.map(listObjectsBuilder.prefix(_)) //requestPayer, overrideAwsConf
    listObjectsBuilder.build()
  }

  def listObject(
    bucket: String,
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None): ListObjectsRequest = {
    val listObjectsBuilder = ListObjectsRequest
      .builder()
      .bucket(bucket)
    prefix.map(listObjectsBuilder.prefix(_))
    delimiter.map(listObjectsBuilder.delimiter(_))
    marker.map(listObjectsBuilder.marker(_))
    maxKeys.map(listObjectsBuilder.maxKeys(_))
    listObjectsBuilder.build()
  }

  def deleteObject(bucket: String, key: String): DeleteObjectRequest = {
    DeleteObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .build()
  }

  def deleteBucket(bucket: String): DeleteBucketRequest = {
    DeleteBucketRequest
      .builder()
      .bucket(bucket)
      .build()
  }

  def createBucket(bucket: String): CreateBucketRequest = {
    CreateBucketRequest
      .builder()
      .bucket(bucket)
      .build()
  }

}
