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

import monix.connect.s3.domain.{
  awsMinChunkSize,
  CopyObjectSettings,
  DefaultCopyObjectSettings,
  DefaultDownloadSettings,
  DefaultUploadSettings,
  DownloadSettings,
  UploadSettings
}
import monix.reactive.{Consumer, Observable, OverflowStrategy}
import monix.execution.Ack
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.observers.Subscriber
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.services.s3.S3AsyncClient
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
  ListObjectsV2Request,
  ListObjectsV2Response,
  NoSuchKeyException,
  PutObjectRequest,
  PutObjectResponse,
  RequestPayer,
  S3Object
}

import scala.jdk.CollectionConverters._

/**
  * An idiomatic Monix integration with Amazon S3.
  *
  * It is built on top of the [[software.amazon.awssdk.services.s3]],
  * which is the reason why all the methods expects an implicit instance
  * of a [[S3AsyncClient]] to be in the scope of the call.
  *
  * ==Example==
  *
  * {{{
  *   import java.time.Duration
  *
  *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
  *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
  *   import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
  *   import software.amazon.awssdk.services.s3.S3AsyncClient
  *
  *   // the exceptions related with concurrency or timeouts from any of the requests
  *   // might be solved by rainsing the `maxConcurrency`, `maxPendingConnectionAcquire` or
  *   // `connectionAcquisitionTimeout` from the underlying netty http client.
  *   // see below an example on how to increase such values.
  *
  *   val httpClient = NettyNioAsyncHttpClient.builder()
  *     .maxConcurrency(500)
  *     .maxPendingConnectionAcquires(50000)
  *     .connectionAcquisitionTimeout(Duration.ofSeconds(60))
  *     .readTimeout(Duration.ofSeconds(60))
  *     .build()
  *
  *   // necessary for all the exposed methods in the [[S3]] object
  *   implicit val s3AsyncClient: S3AsyncClient = S3AsyncClient
  *     .builder()
  *     .httpClient(httpClient)
  *     .credentialsProvider(DefaultCredentialsProvider.create())
  *     .region(AWS_GLOBAL)
  *     .build()
  * }}}
  *
  */
object S3 {

  /**
    * Creates a bucket.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/CreateBucketRequest.Builder.html
    * @param bucket the name of the bucket to be created.
    * @param acl the canned ACL (Access Control List) to apply to the object.
    *            if the service returns an enum value that is not available in the current SDK version, acl will return ObjectCannedACL.UNKNOWN_TO_SDK_VERSION.
    * @param grantFullControl gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
    * @param grantRead allows grantee to read the object data and its metadata.
    * @param grantReadACP allows grantee to read the object ACL.
    * @param grantWriteACP allows grantee to write the ACL for the applicable object.
    * @param s3AsyncClient an implicit instance of a [[S3AsyncClient]].
    * @param objectLockEnabledForBucket specifies whether you want S3 Object Lock to be enabled for the new bucket.
    * @return a [[Task]] with the [[CreateBucketResponse]].
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
    * Creates a bucket given a [[CreateBucketRequest]].
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/CreateBucketRequest.Builder.html
    * @param request an instance of [[CreateBucketRequest]]
    * @param s3AsyncClient an implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] with the create bucket response [[CreateBucketResponse]] .
    */
  def createBucket(request: CreateBucketRequest)(implicit s3AsyncClient: S3AsyncClient): Task[CreateBucketResponse] = {
    Task.from(s3AsyncClient.createBucket(request))
  }

  /**
    * Creates a copy of from an already stored object.
    *
    * @param sourceBucket the name of the source bucket.
    * @param sourceKey the key of the source object.
    * @param destinationBucket the name of the destination bucket.
    * @param destinationKey the key of the destination object.
    * @param copyObjectSettings adds the [[CopyObjectSettings]] on the request copy object request.
    * @param s3AsyncClient an implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] containing the result of the CopyObject operation returned by the service.
    */
  def copyObject(
    sourceBucket: String,
    sourceKey: String,
    destinationBucket: String,
    destinationKey: String,
    copyObjectSettings: CopyObjectSettings = DefaultCopyObjectSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Task[CopyObjectResponse] = {
    val copyRequest =
      S3RequestBuilder.copyObjectRequest(sourceBucket, sourceKey, destinationBucket, destinationKey, copyObjectSettings)
    copyObject(copyRequest)
  }

  /**
    * Creates a copy from an already stored object.
    *
    * @param request the [[CopyObjectRequest]].
    * @param s3AsyncClient an implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] containing the result of the CopyObject operation returned by the service.
    */
  def copyObject(request: CopyObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[CopyObjectResponse] = {
    Task.from(s3AsyncClient.copyObject(request))
  }

  /**
    * Deletes the specified bucket. Which will only happen when it is empty.
    *
    * @note When attempting to delete a bucket that does not exist, Amazon S3 returns a success message, not an error message.
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteBucketRequest.html
    * @param bucket the bucket name to be deleted.
    * @param s3AsyncClient an implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] with the delete bucket response [[DeleteBucketResponse]] .
    */
  def deleteBucket(bucket: String)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteBucketResponse] = {
    Task.from(s3AsyncClient.deleteBucket(S3RequestBuilder.deleteBucket(bucket)))
  }

  /**
    * Deletes the specified bucket. Which will only happen when it is empty.
    *
    * @note When attempting to delete a bucket that does not exist, Amazon S3 returns a success message, not an error message.
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteBucketRequest.html
    * @param request the AWS delete bucket request of type [[DeleteBucketRequest]]
    * @param s3AsyncClient an implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] with the delete bucket response [[DeleteBucketResponse]] .
    */
  def deleteBucket(request: DeleteBucketRequest)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteBucketResponse] = {
    Task.from(s3AsyncClient.deleteBucket(request))
  }

  /**
    * Deletes the specified object.
    * Once deleted, the object can only be restored if versioning was enabled when the object was deleted.
    *
    * @note Once deleted, the object can only be restored if versioning was enabled when the object was deleted.
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteObjectRequest.html
    * @param bucket the bucket name of the object to be deleted.
    * @param key the key of the object to be deleted.
    * @param s3AsyncClient an implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] with the delete object response [[DeleteObjectResponse]] .
    */
  def deleteObject(
    bucket: String,
    key: String,
    bypassGovernanceRetention: Option[Boolean] = None,
    mfa: Option[String] = None,
    requestPayer: Option[String] = None,
    versionId: Option[String] = None)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteObjectResponse] = {
    val request: DeleteObjectRequest =
      S3RequestBuilder.deleteObject(bucket, key, bypassGovernanceRetention, mfa, requestPayer, versionId)
    deleteObject(request)
  }

  /**
    * Deletes the specified object given a [[DeleteBucketRequest]].
    * Once deleted, the object can only be restored if versioning was enabled when the object was deleted.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteObjectRequest.html
    * @param request the AWS delete object request of type [[DeleteObjectRequest]]
    * @param s3AsyncClient implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] with the delete object response [[DeleteObjectResponse]] .
    */
  def deleteObject(request: DeleteObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteObjectResponse] =
    Task.from(s3AsyncClient.deleteObject(request))

  /**
    * Check whether the specified bucket exists or not.
    *
    * @param bucket the bucket name to check its existence
    * @param s3AsyncClient implicit instance of a [[S3AsyncClient]].
    * @return a boolean [[Task]] indicating whether the bucket exists or not.
    */
  def existsBucket(bucket: String)(implicit s3AsyncClient: S3AsyncClient): Task[Boolean] =
    S3.listBuckets().existsL(_.name == bucket)

  /**
    * Checks whether the specified objects exists or not.
    *
    * @param bucket the bucket name of the object to check its existence.
    * @param key the key of the object to be deleted.
    * @param s3AsyncClient implicit instance of a [[S3AsyncClient]].
    * @return a boolean [[Task]] indicating whether the object existed or not.
    */
  def existsObject(bucket: String, key: String)(implicit s3AsyncClient: S3AsyncClient): Task[Boolean] = {
    Task.defer {
      Task.from {
        s3AsyncClient.headObject(S3RequestBuilder.headObjectRequest(bucket, Some(key)))
      }
    }.redeemWith(
      ex =>
        if (ex.isInstanceOf[NoSuchKeyException]) Task.now(false)
        else Task.raiseError(ex),
      _ => Task.now(true))
  }

  /**
    * Downloads an object in a single request as byte array.
    *
    * The only two required fields are the [[bucket]] and [[key]], but it also
    * accepts additional settings for more specific requests, see [[DownloadSettings]].
    *
    * Warn: this method is suitable to be used for downloading small objects,
    * since it performed in a single download request, which might be unsafe
    * when the object is too big to fit in memory or in the http body.
    *
    * @see the safer alternative [[downloadMultipart]] to for downloading objects in parts.
    *
    * ==Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *
    *   //must be properly configured
    *   implicit val client = S3AsyncClient.builder.credentialsProvider(DefaultCredentialsProvider.create()).region(AWS_GLOBAL).build()
    *
    *   val bucket: String = "sample-bucket"
    *   val key: String = "path/to/test.csv"
    *
    *   val t: Task[Array[Byte]] = S3.download(bucket, key, firstNBytes = Some(100))
    * }}}
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/mediastoredata/model/GetObjectRequest.html
    * @param bucket target S3 bucket name of the object to be downloaded.
    * @param key key of the object to be downloaded.
    * @param firstNBytes downloads the first [[firstNBytes]] from the specified object,
    *                    it must be a positive number if defined.
    * @param downloadSettings additional settings to pass to the download object request.
    * @param s3AsyncClient implicit instance of a [[S3AsyncClient]].
    * @return a [[Task]] containing the downloaded object as a byte array.
    */
  def download(
    bucket: String,
    key: String,
    firstNBytes: Option[Int] = None,
    downloadSettings: DownloadSettings = DefaultDownloadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Task[Array[Byte]] = {
    require(firstNBytes.getOrElse(1) > 0, "The number of bytes if defined, must be a positive number.")
    val range = firstNBytes.map(n => s"bytes=0-${n - 1}")
    val request: GetObjectRequest = S3RequestBuilder.getObjectRequest(bucket, key, range, downloadSettings)
    Task
      .from(s3AsyncClient.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]))
      .map(r => r.asByteArray())
  }

  /**
    * Downloads an object as byte array.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/GetObjectRequest.html
    * @param request the AWS get object request of type [[GetObjectRequest]].
    * @param s3AsyncClient An implicit instance of a [[S3AsyncClient]].
    * @return A [[Task]] that contains the downloaded object as a byte array.
    */
  def download(request: GetObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[Array[Byte]] = {
    Task
      .from(s3AsyncClient.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]))
      .map(_.asByteArray())
  }

  /**
    * Safely downloads objects of any size by performing partial download requests.
    * The number of bytes to download per each request is specified by the [[chunkSize]].
    *
    * ==Example==
    *
    * {{{
    *   import monix.reactive.Observable
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *
    *   //must be propelry configured
    *   implicit val client = S3AsyncClient.builder.credentialsProvider(DefaultCredentialsProvider.create()).region(AWS_GLOBAL).build()
    *   val bucket: String = "sample-bucket"
    *   val key: String = "sample-key"
    *
    *   val ob: Observable[Array[Byte]] = S3.downloadMultipart(bucket, key, 2)
    * }}}
    *
    * @param bucket target S3 bucket name of the object to be downloaded.
    * @param key path of the object to be downloaded (excluding the bucket).
    * @param chunkSize amount of bytes to download each part by, being by default (recommended) 5242880 bytes.
    * @param downloadSettings additional settings to pass to the multipart download request.
    * @param s3AsyncClient implicit instance of a [[S3AsyncClient]].
    * @return an [[Observable]] that emits chunks of bytes of size [[chunkSize]] until it completes.
    *         in case the object does not exists an [[Array.emptyByteArray]] is returned,
    *         whereas if the bucket does not exists it return a failed [[Task]] of [[software.amazon.awssdk.services.s3.model.NoSuchBucketException]].
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
    for {
      chunk <- {
        download(getRequest).onErrorHandleWith { ex =>
          sub.onError(ex)
          Task.raiseError(ex)
        }
      }
      ack <- Task.fromFuture(sub.onNext(chunk))
      nextChunk <- {
        ack match {
          case Ack.Continue => {
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
          case Ack.Stop => {
            sub.onComplete()
            Task.unit
          }
        }
      }
    } yield nextChunk
  }

  /**
    * Lists all the existing buckets.
    *
    * ==Example==
    *
    * {{{
    *   import monix.reactive.Observable
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.services.s3.model.Bucket
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *
    *   //must be propelry configured
    *   implicit val client = S3AsyncClient.builder.credentialsProvider(DefaultCredentialsProvider.create()).region(AWS_GLOBAL).build()
    *
    *   val ob: Observable[Bucket] = S3.listBuckets()
    * }}}
    *
    * @param s3AsyncClient implicit instance of a [[S3AsyncClient]].
    * @return an [[Observable]] that emits the list of existing [[Bucket]]s.
    */
  def listBuckets()(implicit s3AsyncClient: S3AsyncClient): Observable[Bucket] = {
    for {
      response <- Observable.fromTaskLike(s3AsyncClient.listBuckets())
      bucket   <- Observable.from(response.buckets().asScala.toList)
    } yield bucket
  }

  /**
    * Returns some or all of the objects in a bucket. You can use the request parameters as selection
    * criteria to return a subset of the objects in a bucket.
    *
    * ==Example==
    *
    * {{{
    *   import monix.reactive.Observable
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.services.s3.model.S3Object
    *
    *   //must be propelry configured
    *   implicit val client = S3AsyncClient.builder.credentialsProvider(DefaultCredentialsProvider.create()).region(AWS_GLOBAL).build()
    *
    *   val bucket = "my-bucket"
    *   val prefix = s"prefix/to/list/keys/"
    *
    *   val s3Objects: Observable[S3Object] = S3.listObjects(bucket, maxTotalKeys = Some(1011), prefix = Some(prefix))
    * }}}
    *
    * To use this operation in an AWS Identity and Access Management (IAM) policy, you must have permissions to perform
    * the <code>s3:ListBucket</code> action. The bucket owner has this permission by default and can grant this
    * permission to others. For more information about permissions, see <a href=
    * "https://docs.aws.amazon.com/AmazonS3/latest/dev/using-with-s3-actions.html#using-with-s3-actions-related-to-bucket-subresources"
    * >Permissions Related to Bucket Subresource Operations</a> and <a
    * href="https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-access-control.html">Managing Access Permissions to Your
    * Amazon S3 Resources</a>.
    *
    * @param bucket target S3 bucket name of the object to be downloaded.
    * @param maxTotalKeys sets the maximum number of keys to be list
    * @param prefix limits the response to keys that begin with the specified prefix.
    * @param requestPayer confirms that the requester knows that she or he will be charged for
    *                     the list objects request in V2 style.
    *                     Bucket owners need not specify this parameter in their requests.
    * @param s3AsyncClient implicit instance of a [[S3AsyncClient]].
    * @return an [[Observable]] that emits the [[S3Object]]s.
    */
  def listObjects(
    bucket: String,
    prefix: Option[String] = None,
    maxTotalKeys: Option[Int] = None,
    requestPayer: Option[RequestPayer] = None)(implicit s3AsyncClient: S3AsyncClient): Observable[S3Object] = {
    require(maxTotalKeys.getOrElse(1) > 0, "The max number of keys, if defined, needs to be higher or equal than 1.")
    val firstRequestSize = maxTotalKeys.map(maxKeys => math.min(maxKeys, domain.awsDefaulMaxKeysList))
    val request: ListObjectsV2Request =
      S3RequestBuilder.listObjectsV2(bucket, prefix = prefix, maxKeys = firstRequestSize, requestPayer = requestPayer)
    listAllObjectsV2(request, maxTotalKeys)
  }

  @InternalApi
  private def listAllObjectsV2(initialRequest: ListObjectsV2Request, maxKeys: Option[Int])(
    implicit s3AsyncClient: S3AsyncClient): Observable[S3Object] = {

    def prepareNextRequest(continuationToken: String, pendingKeys: Option[Int]): ListObjectsV2Request = {
      val requestBuilder = initialRequest.toBuilder.continuationToken(continuationToken)
      pendingKeys.map { n =>
        val nextMaxkeys = math.min(n, domain.awsDefaulMaxKeysList)
        requestBuilder.maxKeys(nextMaxkeys)
      }.getOrElse(domain.awsDefaulMaxKeysList)
      requestBuilder.build()
    }

    def nextListRequest(
      sub: Subscriber[ListObjectsV2Response],
      pendingKeys: Option[Int],
      request: ListObjectsV2Request): Task[Unit] = {

      for {
        r <- {
          Task.from(s3AsyncClient.listObjectsV2(request)).onErrorHandleWith { ex =>
            sub.onError(ex)
            Task.raiseError(ex)
          }
        }
        ack <- Task.deferFuture(sub.onNext(r))
        next <- {
          ack match {
            case Ack.Continue => {
              if (r.isTruncated && (r.nextContinuationToken != null)) {
                val updatedPendingKeys = pendingKeys.map(_ - r.contents.size)
                updatedPendingKeys match {
                  case Some(pendingKeys) =>
                    if (pendingKeys <= 0) { sub.onComplete(); Task.unit }
                    else
                      nextListRequest(
                        sub,
                        updatedPendingKeys,
                        prepareNextRequest(r.nextContinuationToken, updatedPendingKeys))
                  case None =>
                    nextListRequest(
                      sub,
                      updatedPendingKeys,
                      prepareNextRequest(r.nextContinuationToken, updatedPendingKeys))
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
    * Uploads a new object to the specified Amazon S3 bucket.
    *
    * ==Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import software.amazon.awssdk.services.s3.model.PutObjectResponse
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.services.s3.model.S3Object
    *
    *   //must be propelry configured
    *   implicit val client = S3AsyncClient.builder.credentialsProvider(DefaultCredentialsProvider.create()).region(AWS_GLOBAL).build()
    *
    *   val bucket: String = "sample-bucket"
    *   val key: String = "sample/s3/object"
    *   val content: Array[Byte] = "Whatever your content is".getBytes()
    *
    *   val t: Task[PutObjectResponse] = S3.upload(bucket, key, content)
    * }}}
    *
    * @param bucket the bucket where this request will upload a new object to
    * @param key key under which to store the new object
    * @param content text content to be uploaded
    * @param s3AsyncClient implicit instance of [[S3AsyncClient]]
    * @return response from the put object http request as [[PutObjectResponse]]
    */
  def upload(bucket: String, key: String, content: Array[Byte], uploadSettings: UploadSettings = DefaultUploadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Task[PutObjectResponse] = {
    val actualLength: Long = content.length.toLong
    val request: PutObjectRequest =
      S3RequestBuilder.putObjectRequest(bucket, key, Some(actualLength), uploadSettings)
    Task.from(s3AsyncClient.putObject(request, AsyncRequestBody.fromBytes(content)))
  }

  /**
    * Uploads a new object to the specified Amazon S3 bucket.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/PutObjectRequest.html
    * @param request instance of [[PutObjectRequest]]
    * @param content content to be uploaded
    * @param s3AsyncClient implicit instance of a [[S3AsyncClient]].
    * @return the response from the http put object request as [[PutObjectResponse]].
    */
  def upload(request: PutObjectRequest, content: Array[Byte])(
    implicit
    s3AsyncClient: S3AsyncClient): Task[PutObjectResponse] =
    Task.from(s3AsyncClient.putObject(request, AsyncRequestBody.fromBytes(content)))

  /**
    * Uploads an S3 object by making multiple http requests (parts) of the received chunks of bytes.
    *
    * ==Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.reactive.{Observable, Consumer}
    *   import monix.connect.s3.S3
    *   import monix.execution.Scheduler.Implicits.global
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *
    *   //must be propelry configured
    *   implicit val client = S3AsyncClient.builder.credentialsProvider(DefaultCredentialsProvider.create()).region(AWS_GLOBAL).build()
    *
    *   val bucket: String = "sample-bucket"
    *   val key: String = "sample/key/to/s3/object"
    *   val content: Array[Byte] = "Hello World!".getBytes
    *
    *   val uploadConsumer: Consumer[Array[Byte], CompleteMultipartUploadResponse] = S3.uploadMultipart(bucket, key)
    *
    *   val t: Task[CompleteMultipartUploadResponse] = Observable.pure(content).consumeWith(uploadConsumer)
    * }}}
    *
    * @param bucket the bucket name where the object will be stored
    * @param key the key where the object will be stored.
    * @param minChunkSize size of the chunks (parts) that will be sent in the http body. (the minimum size is set by default, don't use a lower one)
    * @param s3AsyncClient implicit instance of the s3 client of type [[S3AsyncClient]]
    * @return the confirmation of the multipart whole upload as [[CompleteMultipartUploadResponse]].
    */
  def uploadMultipart(
    bucket: String,
    key: String,
    minChunkSize: Int = awsMinChunkSize,
    uploadSettings: UploadSettings = DefaultUploadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Consumer[Array[Byte], CompleteMultipartUploadResponse] =
    new MultipartUploadSubscriber(bucket, key, minChunkSize, uploadSettings)

}
