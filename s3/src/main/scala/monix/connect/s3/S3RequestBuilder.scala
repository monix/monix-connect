/*
 * Copyright (c) 2014-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.s3

import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadRequest,
  CompletedMultipartUpload,
  CompletedPart,
  CreateBucketRequest,
  CreateMultipartUploadRequest,
  DeleteBucketRequest,
  DeleteObjectRequest,
  ListObjectsRequest,
  ListObjectsV2Request,
  PutObjectRequest,
  UploadPartRequest,
  UploadPartResponse
}

import scala.jdk.CollectionConverters._

/**
* A class that provides converter methods that given the required set of parameters for that
  * conversion, it builds the relative AWS java object.
  */
private[s3] object S3RequestBuilder {

  /**
    * A builder for [[PutObjectRequest]]
    */
  def putObjectRequest(bucket: String, key: String, contentLenght: Long, contentType: Option[String]) =
    PutObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .contentLength(contentLenght)
      .contentType(contentType.getOrElse("plain/text"))
      .build()

  /**
    * A builder for [[UploadPartRequest]]
    */
  def uploadPartRequest(bucketName: String, key: String, partN: Int, uploadId: String, contentLenght: Long) =
    UploadPartRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .partNumber(partN)
      .uploadId(uploadId)
      .contentLength(contentLenght)
      .build()

  /**
    * A builder for [[CompletedPart]]
    */
  def completedPart(partN: Int, uploadPartResp: UploadPartResponse): CompletedPart =
    CompletedPart.builder().partNumber(partN).eTag(uploadPartResp.eTag()).build()

  /**
    * A builder for [[CreateMultipartUploadRequest]]
    */
  def createMultipartUploadRequest(bucketName: String, key: String, contentType: Option[String]): CreateMultipartUploadRequest =
    CreateMultipartUploadRequest
      .builder()
      .bucket(bucketName)
      .key(key)
      .contentType(contentType.getOrElse("plain/text"))
      .build()

  /**
    * A builder for [[CompleteMultipartUploadRequest]]
    */
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

  /**
    * A builder for [[ListObjectsRequest]]
    */
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

  /**
    * A builder for [[ListObjectsV2Request]]
    */
  def listObjectV2(
    bucket: String,
    continuationToken: Option[String] = None,
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None): ListObjectsV2Request = {
    val listObjectsBuilder = ListObjectsV2Request.builder().bucket(bucket)
    continuationToken.map(listObjectsBuilder.continuationToken(_))
    delimiter.map(listObjectsBuilder.delimiter(_))
    maxKeys.map(listObjectsBuilder.maxKeys(_))
    prefix.map(listObjectsBuilder.prefix(_)) //requestPayer, overrideAwsConf
    listObjectsBuilder.build()
  }

  /**
    * A builder for [[DeleteObjectRequest]]
    */
  def deleteObject(bucket: String, key: String): DeleteObjectRequest = {
    DeleteObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .build()
  }

  /**
    * A builder for [[DeleteBucketRequest]]
    */
  def deleteBucket(bucket: String): DeleteBucketRequest = {
    DeleteBucketRequest
      .builder()
      .bucket(bucket)
      .build()
  }

  /**
    * A builder for [[CreateBucketRequest]]
    */
  def createBucket(bucket: String): CreateBucketRequest = {
    CreateBucketRequest
      .builder()
      .bucket(bucket)
      .build()
  }

}
