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

import java.nio.ByteBuffer

import monix.reactive.Consumer
import monix.execution.Scheduler
import monix.eval.Task
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadResponse, CreateBucketResponse, DeleteBucketResponse, DeleteObjectResponse, GetObjectRequest, ListObjectsResponse, ListObjectsV2Response, PutObjectRequest, PutObjectResponse}

import scala.jdk.FutureConverters._

object S3 {

  /**
    *
    * @param bucketName
    * @param key
    * @param s3Client
    * @return
    */
  def getObject(bucketName: String, key: String)(implicit s3Client: S3AsyncClient): Task[Array[Byte]] = {
    val getObjectrequest = GetObjectRequest.builder().bucket(bucketName).key(key).build()
    Task.from(s3Client.getObject(getObjectrequest, new MonixS3AsyncResponseTransformer)).flatten.map(_.array)
  }

  /**
    *
    * @param bucketName
    * @param key
    * @param content
    * @param contentLength
    * @param contentType
    * @param s3Client
    * @param s
    * @return
    */
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

  /**
  * Uploads an S3 object by making multiple http requests (parts) of the received chunks of bytes.
   * @param bucketName The bucket name where the object will be stored
   * @param key Path where the object will be stored.
   * @param chunkSize Size of the chunks (parts) that will be sent in the http body. (the minimum size is set by default, don't use a lower one)
   * @param contentType Content type in which the http request will be sent.
   * @param s3Client Implicit instance of the s3 client of type [[S3AsyncClient]]
   * @return Returns the confirmation of the multipart upload as [[CompleteMultipartUploadResponse]]
   */
  def multipartUpload(bucketName: String, key: String, chunkSize: Int = awsMinChunkSize, contentType: Option[String] = None)(
    implicit
    s3Client: S3AsyncClient): Consumer[Array[Byte], Task[CompleteMultipartUploadResponse]] = {
    new MultipartUploadConsumer(bucketName, key, chunkSize, contentType)
  }

  /**
    *
    * @param bucket
    * @param key
    * @param s3Client
    * @return
    */
  def deleteObject(bucket: String, key: String)(implicit s3Client: S3AsyncClient): Task[DeleteObjectResponse] = {
    Task.from(s3Client.deleteObject(S3RequestBuilder.deleteObject(bucket, key)))
  }

  /**
    *
    * @param bucket
    * @param s3Client
    * @return
    */
  def deleteBucket(bucket: String)(implicit s3Client: S3AsyncClient): Task[DeleteBucketResponse] = {
    Task.from(s3Client.deleteBucket(S3RequestBuilder.deleteBucket(bucket)))
  }

  /**
    *
    * @param bucket
    * @param s3Client
    * @return
    */
  def createBucket(bucket: String)(implicit s3Client: S3AsyncClient): Task[CreateBucketResponse] = {
    Task.from(s3Client.createBucket(S3RequestBuilder.createBucket(bucket)))
  }

  /**
    *
    * @param bucket
    * @param delimiter
    * @param marker
    * @param maxKeys
    * @param prefix
    * @param s3Client
    * @return
    */
  def listObjects(
    bucket: String,
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None)(implicit s3Client: S3AsyncClient): Task[ListObjectsResponse] = {
    val request =
      S3RequestBuilder.listObject(bucket, delimiter, marker, maxKeys, prefix)
    Task.from(s3Client.listObjects(request))
  }


  /**
  *
    * @param bucket
    * @param continuationToken
    * @param delimiter
    * @param marker
    * @param maxKeys
    * @param prefix
    * @param s3Client
    * @return
    */
  def listObjectsV2(
    bucket: String,
    continuationToken: Option[String] = None,
    delimiter: Option[String] = None,
    marker: Option[String] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None)(implicit s3Client: S3AsyncClient): Task[ListObjectsV2Response] = {
    val request = S3RequestBuilder.listObjectV2(bucket, continuationToken, delimiter, marker, maxKeys, prefix)
    Task.from(s3Client.listObjectsV2(request))
  }

}
