/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
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

import monix.connect.s3.domain.{
  awsMinChunkSize,
  CopyObjectSettings,
  DefaultDownloadSettings,
  DefaultUploadSettings,
  DownloadSettings,
  UploadSettings
}
import monix.reactive.{Consumer, Observable, OverflowStrategy}
import monix.execution.{Ack, Scheduler}
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.services.s3.{model, S3AsyncClient}
import software.amazon.awssdk.services.s3.model.{
  Bucket,
  CompleteMultipartUploadResponse,
  CopyObjectRequest,
  CopyObjectResponse,
  CreateBucketRequest,
  CreateBucketResponse,
  DeleteBucketRequest,
  DeleteBucketResponse,
  DeleteObjectRequest,
  DeleteObjectResponse,
  GetObjectRequest,
  GetObjectResponse,
  HeadObjectRequest,
  HeadObjectResponse,
  ListBucketsRequest,
  ListObjectsRequest,
  ListObjectsResponse,
  ListObjectsV2Request,
  ListObjectsV2Response,
  ListPartsRequest,
  ListPartsResponse,
  NoSuchKeyException,
  PutObjectRequest,
  PutObjectResponse,
  RequestPayer,
  S3Object,
  ServerSideEncryption
}

import scala.jdk.CollectionConverters._

/**
  * An idiomatic monix service client for Amazon S3.
  *
  * It is built on top of the [[software.amazon.awssdk.services.s3]], that's the reason why
  * all the methods expects an implicit instance of a [[S3AsyncClient]] to be in the scope of the call.
  *
  * Each of the methods expects at least the required parameters to build the AWS S3 request plus
  * optionally the most relevant ones that allow advanced settings such as encription and request payer etc.
  * On the other hand all methods but multipart upload accepts the native aws requests to be passed.
  */
object S3 {

  /**
    * Executes the request to create a bucket given a [[bucket]] name.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/CreateBucketRequest.Builder.html
    * @param bucket                     The name of the bucket to be created.
    * @param acl                        The canned ACL (Access Control List) to apply to the object. For more information
    *                                   If the service returns an enum value that is not available in the current SDK version, acl will return ObjectCannedACL.UNKNOWN_TO_SDK_VERSION.
    *                                   The raw value returned by the service is available from aclAsString().
    * @param grantFullControl           Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
    * @param grantRead                  Allows grantee to read the object data and its metadata.
    * @param grantReadACP               Allows grantee to read the object ACL.
    * @param grantWriteACP              Allows grantee to write the ACL for the applicable object.
    * @param s3AsyncClient                   An implicit instance of a [[S3AsyncClient]].
    * @param objectLockEnabledForBucket Specifies whether you want S3 Object Lock to be enabled for the new bucket.
    * @return A [[Task]] with the create bucket response [[CreateBucketResponse]] .
    */
  def createBucket(
    bucket: String,
    acl: Option[String] = None,
    grantFullControl: Option[String] = None,
    grantRead: Option[String] = None,
    grantReadACP: Option[String] = None,
    grantWrite: Option[String] = None,
    grantWriteACP: Option[String] = None,
    objectLockEnabledForBucket: Option[Boolean] = None)(
    implicit
    s3AsyncClient: S3AsyncClient): Task[CreateBucketResponse] = {
    Task.from(
      s3AsyncClient.createBucket(
        S3RequestBuilder.createBucket(
          bucket,
          acl,
          grantFullControl,
          grantRead,
          grantReadACP,
          grantWrite,
          grantWriteACP,
          objectLockEnabledForBucket)))
  }

  /**
    * This method creates a bucket given a [[CreateBucketRequest]].
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/CreateBucketRequest.Builder.html
    * @param request  An instance of [[CreateBucketRequest]]
    * @param s3AsyncClient An implicit instance of a [[S3AsyncClient]].
    * @return A [[Task]] with the create bucket response [[CreateBucketResponse]] .
    */
  def createBucket(request: CreateBucketRequest)(implicit s3AsyncClient: S3AsyncClient): Task[CreateBucketResponse] = {
    Task.from(s3AsyncClient.createBucket(request))
  }

  //todo tests
  def copyObject(
    sourceBucket: String,
    sourceKey: String,
    destinationBucket: String,
    destinationKey: String,
    copyObjectSettings: CopyObjectSettings)(implicit s3AsyncClient: S3AsyncClient): Task[CopyObjectResponse] = {
    val copyRequest =
      S3RequestBuilder.copyObjectRequest(sourceBucket, sourceKey, destinationBucket, destinationKey, copyObjectSettings)
    Task.from {
      s3AsyncClient.copyObject(copyRequest)
    }
  }

  //todo
  def copyObject(request: CopyObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[CopyObjectResponse] = {
    Task.from(s3AsyncClient.copyObject(request))
  }

  /**
    * Provides options for deleting a specified bucket. Amazon S3 buckets can only be deleted when empty.
    *
    * @note When attempting to delete a bucket that does not exist, Amazon S3 returns a success message, not an error message.
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteBucketRequest.html
    * @param bucket   The bucket name to be deleted.
    * @param s3AsyncClient An implicit instance of a [[S3AsyncClient]].
    * @return A [[Task]] with the delete bucket response [[DeleteBucketResponse]] .
    */
  def deleteBucket(bucket: String)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteBucketResponse] = {
    Task.from(s3AsyncClient.deleteBucket(S3RequestBuilder.deleteBucket(bucket)))
  }

  /**
    * //todo test deleting non existing bucket
    * Provides options for deleting a specified bucket. Amazon S3 buckets can only be deleted when empty.
    *
    * @note When attempting to delete a bucket that does not exist, Amazon S3 returns a success message, not an error message.
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteBucketRequest.html
    * @param request  The AWS delete bucket request of type [[DeleteBucketRequest]]
    * @param s3AsyncClient An implicit instance of a [[S3AsyncClient]].
    * @return A [[Task]] with the delete bucket response [[DeleteBucketResponse]] .
    */
  def deleteBucket(request: DeleteBucketRequest)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteBucketResponse] = {
    Task.from(s3AsyncClient.deleteBucket(request))
  }

  /**
    * Deletes a specified object in a specified bucket.
    *
    * @note Once deleted, the object can only be restored if versioning was enabled when the object was deleted.
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteObjectRequest.html
    * @param bucket   The bucket name of the object to be deleted.
    * @param key      Key of the object to be deleted.
    * @param s3AsyncClient An implicit instance of a [[S3AsyncClient]].
    * @return A [[Task]] with the delete object response [[DeleteObjectResponse]] .
    */
  def deleteObject(
    bucket: String,
    key: String,
    bypassGovernanceRetention: Option[Boolean] = None,
    mfa: Option[String] = None,
    requestPayer: Option[String] = None,
    versionId: Option[String])(implicit s3AsyncClient: S3AsyncClient): Task[DeleteObjectResponse] = {
    val request: DeleteObjectRequest =
      S3RequestBuilder.deleteObject(bucket, key, bypassGovernanceRetention, mfa, requestPayer, versionId)
    Task.from(s3AsyncClient.deleteObject(request))
  }

  /**
    * Deletes a specified object in a specified bucket.
    * Once deleted, the object can only be restored if versioning was enabled when the object was deleted.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteObjectRequest.html
    * @param request  The AWS delete object request of type [[DeleteObjectRequest]]
    * @param s3AsyncClient An implicit instance of a [[S3AsyncClient]].
    * @return A [[Task]] with the delete object response [[DeleteObjectResponse]] .
    */
  def deleteObject(request: DeleteObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteObjectResponse] = {
    Task.from(s3AsyncClient.deleteObject(request))
  }

  /**
    *
    * @param bucketName
    * @param key
    * @param s3AsyncClient
    * @return
    */
  def existsObject(bucketName: String, key: String)(implicit s3AsyncClient: S3AsyncClient): Task[Boolean] = {
    Task.defer {
      Task.from {
        s3AsyncClient.headObject(S3RequestBuilder.headObjectRequest(bucketName, Some(key)))
      }
    }.redeemWith(
      ex =>
        if (ex.isInstanceOf[NoSuchKeyException]) Task.now(false)
        else Task.raiseError(ex),
      _ => Task.now(true))
  }

  def existsBucket(bucketName: String)(implicit s3AsyncClient: S3AsyncClient): Task[Boolean] =
    S3.listBuckets().existsL(_.name == bucketName)

  /**
    * Downloads an Amazon S3 object as byte array.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/GetObjectRequest.html
    * @param request  The AWS get object request of type [[GetObjectRequest]].
    * @param s3AsyncClient An implicit instance of a [[S3AsyncClient]].
    * @return A [[Task]] that contains the downloaded object as a byte array.
    */
  def download(request: GetObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[Array[Byte]] = {
    Task
      .from(s3AsyncClient.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]))
      .map(_.asByteArray())
  }

  /**
    * Downloads an Amazon S3 object as byte array.
    * The only two required fields are the [[bucket]] and [[key]], but it also
    * accepts additional / optional settings for more specific requests.
    *
    * todo
    * The method performs a single downloads request, which might be unsafe
    * since the object can be too big to send it in the http body or to even fit in memory (OOM).
    * The first N bytes can from the object, it is possible using the [[range]] parameter.
    * As a safe alternative see the method [[downloadMultipart]].
    * However this method could also for example if we do only want to download
    *
    *
    * Example:
    * {{{
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import monix.eval.Task
    *
    *   implicit val s3AsyncClient: S3AsyncClient = ???
    *
    *   val bucketName: String= "sampleBucket"
    *   val key: String= "path/to/test.csv"
    *
    *   val t: Task[Array[Byte]] = S3.getObject(bucketName, key, range = Some(100L))
    * }}}
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/mediastoredata/model/GetObjectRequest.html
    * @param bucket               target S3 bucket name of the object to be downloaded.
    * @param key                  key of the object to be downloaded.
    * @param numberOfBytes        downloads the first [[numberOfBytes]] from the specified object,
    *                             it must be a positive number if defined.
    * @param downloadSettings     additional settings to pass to the download object request.
    * @param s3Async3Client       implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] containing the downloaded object as a byte array.
    */
  def download(
    bucket: String,
    key: String,
    numberOfBytes: Option[Int] = None,
    downloadSettings: DownloadSettings = DefaultDownloadSettings)(
    implicit
    s3Async3Client: S3AsyncClient): Task[Array[Byte]] = {
    require(numberOfBytes.getOrElse(1) > 0, "The number of bytes if defined, must be a positive number.")
    val range = numberOfBytes.map(n => s"bytes=0-${n - 1}")
    val request: GetObjectRequest = S3RequestBuilder.getObjectRequest(bucket, key, range, downloadSettings)
    Task
      .from(s3Async3Client.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]))
      .map(r => r.asByteArray())
  }

  /**
    * A method that safely downloads objects of any size by performing multiple partial requests.
    * The number of bytes to download per each request is specified by the [[chunkSize]].
    *
    * @param bucket               target S3 bucket name of the object to be downloaded.
    * @param key                  key of the object to be downloaded.
    * @param chunkSize            amount of bytes to download each part by, being by default (recommended) 5242880 bytes.
    * @param downloadSettings     additional settings to pass to the multipart download request.
    * @param s3AsyncClient
    * @return
    */
  def downloadMultipart(
    bucket: String,
    key: String,
    chunkSize: Long = domain.awsMinChunkSize,
    downloadSettings: DownloadSettings = DefaultDownloadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Observable[Array[Byte]] = {
    require(chunkSize > 0, "Chunk size must be a positive number.")
    val resizedChunk: Long = chunkSize - 1L
    val firstChunkRange = s"bytes=0-${resizedChunk}"
    val initialRequest: GetObjectRequest =
      S3RequestBuilder.getObjectRequest(bucket, key, Some(firstChunkRange), downloadSettings)

    for {
      totalObjectSize <- listObjects(bucket, prefix = Some(key), maxTotalKeys = Some(1)).head.map(_.size)
      o2 <- {
        Observable.create[Array[Byte]](OverflowStrategy.Unbounded) { sub =>
          downloadChunk(sub, totalObjectSize, chunkSize, initialRequest, 0).runToFuture(sub.scheduler)
        }
      }
    } yield o2

  }

  @InternalApi
  private def downloadChunk(
    sub: Subscriber[Array[Byte]],
    totalSize: Long,
    chunkSize: Long,
    getRequest: GetObjectRequest,
    offset: Int)(implicit s3AsyncClient: S3AsyncClient): Task[Unit] = {
    download(getRequest).flatMap { bytes => Task.parZip2(Task.fromFuture(sub.onNext(bytes)), Task.now(bytes)) }.flatMap {
      case (Ack.Continue, chunk) => {
        val nextOffset = offset + chunk.size
        if (nextOffset < totalSize) {
          val nextRange = s"bytes=${nextOffset}-${nextOffset + chunkSize}"
          val nextRequest = getRequest.toBuilder.range(nextRange).build()
          downloadChunk(sub, totalSize, chunkSize, nextRequest, nextOffset)
        } else {
          sub.onComplete()
          Task.unit
        }
      }
      case (Ack.Stop, _) => {
        sub.onComplete()
        Task.unit
      };
    }

  }

  /**
    * @param s3Client
    * @return
    */
  def listBuckets()(implicit s3Client: S3AsyncClient): Observable[Bucket] = {
    for {
      response <- Observable.fromTaskLike(s3Client.listBuckets())
      bucket   <- Observable.from(response.buckets().asScala.toList)
    } yield bucket
  }

  /**
    *
    * @param bucket
    * @param maxTotalKeys
    * @param prefix
    * @param delimiter
    * @param requestPayer
    * @param s3AsyncClient
    * @return
    */
  def listObjects(
    bucket: String,
    prefix: Option[String] = None,
    maxTotalKeys: Option[Int] = None,
    limitObjectsPerRequest: Option[Int] = None,
    delimiter: Option[String] = None,
    requestPayer: Option[String] = None)(implicit s3AsyncClient: S3AsyncClient): Observable[S3Object] = {
    require(maxTotalKeys.getOrElse(1) > 0, "The max number of keys, if defined, need to be higher or equal than 1.")
    val firstRequestSize = (maxTotalKeys, limitObjectsPerRequest) match {
      case (Some(a), Some(b)) => Some(math.min(a, b))
      case (a, b) => a.orElse(b)
    }
    val request: ListObjectsV2Request =
      S3RequestBuilder.listObjectsV2(bucket, prefix = prefix, maxKeys = firstRequestSize)
    listAllObjectsV2(request, maxTotalKeys, limitObjectsPerRequest)
  }

  @InternalApi
  private def listAllObjectsV2(
    initialRequest: ListObjectsV2Request,
    maxKeys: Option[Int],
    limitPerRequest: Option[Int])(implicit s3AsyncClient: S3AsyncClient): Observable[S3Object] = {

    def nextListRequest(
      sub: Subscriber[ListObjectsV2Response],
      pendingKeys: Option[Int],
      request: ListObjectsV2Request): Task[Unit] = {
      def prepareNextRequest(continuationToken: String): ListObjectsV2Request = {
        val requestBuilder = initialRequest.toBuilder.continuationToken(continuationToken)
        limitPerRequest.map(requestBuilder.maxKeys(_))
        requestBuilder.build()
      }
      for {
        r   <- Task.from(s3AsyncClient.listObjectsV2(request))
        ack <- Task.deferFuture(sub.onNext(r))
        next <- {
          ack match {
            case Ack.Continue => {
              if (r.isTruncated && (r.continuationToken() != null)) {
                val updatedPendingKeys = pendingKeys.map(_ - r.contents.size)
                updatedPendingKeys match {
                  case Some(pendingKeys) =>
                    if (pendingKeys <= 0) { sub.onComplete(); Task.unit }
                    else nextListRequest(sub, updatedPendingKeys, prepareNextRequest(r.continuationToken()))
                  case None =>
                    nextListRequest(sub, updatedPendingKeys, prepareNextRequest(r.continuationToken()))
                }
              } else {
                sub.onComplete()
                Task.unit
              }
            }
            case Ack.Stop => Task.unit
          }
        }
      } yield next
    }

    for {
      listResponse <- {
        Observable.create[ListObjectsV2Response](OverflowStrategy.Unbounded) { sub =>
          nextListRequest(sub, maxKeys, initialRequest).runToFuture(sub.scheduler)
        }
      }
      s3Object <- Observable.from(listResponse.contents.asScala.toList)
    } yield s3Object

  }

  /**
    * Uploads an S3 object by making multiple http requests (parts) of the received chunks of bytes.
    *
    * ==Example==
    *
    * {
    *   import monix.eval.Task
    *   import monix.reactive.{Observable, Consumer}
    *   import monix.connect.s3.S3
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse
    *
    *   implicit val s3AsyncClient: S3AsyncClient = ???
    *
    *   val bucketName: String = "sampleBucketName"
    *   val key: String = "sample/key/to/s3/object"
    *   val content: Array[Byte] = "Hello World!".getBytes
    *
    *   val multipartUploadConsumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] = S3.multipartUpload(bucketName, key)(s3AsyncClient)
    *
    *   val t: Task[CompleteMultipartUploadResponse] = Observable.pure(content).consumeWith(multipartUploadConsumer)
    * }
    *
    * @param bucket                  The bucket name where the object will be stored
    * @param key                     Key where the object will be stored.
    * @param minChunkSize               Size of the chunks (parts) that will be sent in the http body. (the minimum size is set by default, don't use a lower one)
    * @param s3AsyncClient                Implicit instance of the s3 client of type [[S3AsyncClient]]
    * @return Returns the confirmation of the multipart upload as [[CompleteMultipartUploadResponse]]
    */
  def multipartUpload(
    bucket: String,
    key: String,
    minChunkSize: Int = awsMinChunkSize,
    uploadSettings: UploadSettings = DefaultUploadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Consumer[Array[Byte], CompleteMultipartUploadResponse] = {
    new MultipartUploadSubscriber(bucket, key, minChunkSize, uploadSettings)
  }

  /**
    * Uploads a new object to the specified Amazon S3 bucket.
    *
    * ==Example==
    *
    * {
    *    import monix.eval.Task
    *    import software.amazon.awssdk.services.s3.S3AsyncClient
    *    import software.amazon.awssdk.services.s3.model.PutObjectResponse
    *    import monix.execution.Scheduler
    *
    *    implicit val scheduler: Scheduler = ???
    *    implicit val s3AsyncClient: S3AsyncClient = ???
    *
    *    val bucket: String = "sampleBucketName"
    *    val key: String = "sample/s3/object"
    *    val content: Array[Byte] = "Whatever".getBytes()
    *
    *    val t: Task[PutObjectResponse] = S3.putObject(bucket, key, content)(s3AsyncClient, scheduler)
    * }
    *
    * @param bucketName    Bucket where this request will upload a new object to
    * @param key           Key under which to store the new object
    * @param content       Text content to be uploaded.
    * @param s3AsyncClient                An implicit instance of [[S3AsyncClient]].
    * @return The response from the put object http request as [[PutObjectResponse]].
    */
  def upload(
    bucketName: String,
    key: String,
    content: Array[Byte],
    uploadSettings: UploadSettings = DefaultUploadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient,
    scheduler: Scheduler): Task[PutObjectResponse] = {
    val actualLength: Long = content.length.toLong
    val request: PutObjectRequest =
      S3RequestBuilder.putObjectRequest(bucketName, key, Some(actualLength), uploadSettings)
    Task.from(s3AsyncClient.putObject(request, AsyncRequestBody.fromBytes(content)))
  }

  /**
    * Uploads a new object to the specified Amazon S3 bucket.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/PutObjectRequest.html
    * @param request   An instance of [[PutObjectRequest]]
    * @param content   Text content to be uploaded. Use this property if you want to upload plaintext to S3. The content type will be set to 'text/plain' automatically.
    * @param s3AsyncClient  An implicit instance of a [[S3AsyncClient]].
    * @param scheduler An implicit instance of monix [[Scheduler]].
    * @return It returns the response from the http put object request as [[PutObjectResponse]].
    */
  def upload(request: PutObjectRequest, content: Array[Byte])(
    implicit
    s3AsyncClient: S3AsyncClient,
    scheduler: Scheduler): Task[PutObjectResponse] = {
    Task.from(s3AsyncClient.putObject(request, AsyncRequestBody.fromBytes(content)))
  }

}
