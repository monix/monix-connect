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

import cats.effect.Resource
import monix.connect.aws.auth.AppConf
import monix.connect.s3.domain.{
  awsMinChunkSize,
  CopyObjectSettings,
  DefaultCopyObjectSettings,
  DefaultDownloadSettings,
  DefaultUploadSettings,
  DownloadSettings,
  UploadSettings
}
import monix.reactive.{Consumer, Observable}
import monix.eval.Task
import monix.execution.annotations.{Unsafe, UnsafeBecauseImpure}
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{
  Bucket,
  BucketCannedACL,
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
  NoSuchKeyException,
  PutObjectRequest,
  PutObjectResponse,
  RequestPayer,
  S3Object
}

import scala.jdk.CollectionConverters._

/**
  * Singleton object provides builders for [[S3]].
  *
  * ==Example==
  *
  * {{{
  * import monix.eval.Task
  * import software.amazon.awssdk.services.s3.model.NoSuchKeyException
  *
  * val bucket = "my-bucket"
  * val key = "my-key"
  * val content = "my-content"
  *
  * def runS3App(s3: S3): Task[Array[Byte]] = {
  *   for {
  *     _ <- s3.createBucket(bucket)
  *     _ <- s3.upload(bucket, key, content.getBytes)
  *     existsObject <- s3.existsObject(bucket, key)
  *     download <- {
  *       if(existsObject) s3.download(bucket, key)
  *       else Task.raiseError(NoSuchKeyException.builder().build())
  *     }
  *   } yield download
  * }
  *
  * val t = S3.fromConfig.use(s3 => runS3App(s3))
  * }}}
  *
  */
object S3 { self =>

  /**
    * Creates a [[Resource]] that will use the values from a
    * configuration file to allocate and release a [[S3]].
    * Thus, the api expects an `application.conf` file to be present
    * in the `resources` folder.
    *
    * @see how does the expected `.conf` file should look like
    *      https://github.com/monix/monix-connect/blob/master/aws-auth/src/main/resources/reference.conf`
    *
    * @see the cats effect resource data type: https://typelevel.org/cats-effect/datatypes/resource.html
    *
    * @return a [[Resource]] of [[Task]] that allocates and releases [[S3]].
    */
  def fromConfig: Resource[Task, S3] = {
    Resource.make {
      for {
        clientConf  <- Task.eval(AppConf.loadOrThrow)
        asyncClient <- Task.now(AsyncClientConversions.fromMonixAwsConf(clientConf.monixAws))
      } yield {
        self.createUnsafe(asyncClient)
      }
    } { _.close }
  }

  /**
    * Creates a [[Resource]] that will use the passed
    * AWS configurations to allocate and release [[S3]].
    * Thus, the api expects an `application.conf` file to be present
    * in the `resources` folder.
    *
    * @see how does the expected `.conf` file should look like
    *      https://github.com/monix/monix-connect/blob/master/aws-auth/src/main/resources/reference.conf`
    *
    * @see the cats effect resource data type: https://typelevel.org/cats-effect/datatypes/resource.html
    *
    * ==Example==
    *
    * {{{
    *   import cats.effect.Resource
    *   import monix.eval.Task
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region
    *
    *   val defaultCredentials = DefaultCredentialsProvider.create()
    *   val s3Resource: Resource[Task, S3] = S3.create(defaultCredentials, Region.AWS_GLOBAL)
    * }}}
    *
    * @param credentialsProvider Strategy for loading credentials and authenticate to AWS S3
    * @param region An Amazon Web Services region that hosts a set of Amazon services.
    * @param endpoint The endpoint with which the SDK should communicate.
    * @param httpClient Sets the [[SdkAsyncHttpClient]] that the SDK service client will use to make HTTP calls.
    * @return a [[Resource]] of [[Task]] that allocates and releases [[S3]].
    **/
  def create(
    credentialsProvider: AwsCredentialsProvider,
    region: Region,
    endpoint: Option[String] = None,
    httpClient: Option[SdkAsyncHttpClient] = None): Resource[Task, S3] = {
    Resource.make {
      Task.eval {
        val asyncClient = AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
        createUnsafe(asyncClient)
      }
    } { _.close }
  }

  /**
    * Creates a instance of [[S3]] out of a [[S3AsyncClient]].
    *
    * It provides a fast forward access to the [[S3]] that avoids
    * dealing with [[Resource]].
    *
    * Unsafe because the state of the passed [[S3AsyncClient]] is not guaranteed,
    * it can either be malformed or closed, which would result in underlying failures.
    *
    * @see [[S3.fromConfig]] and [[S3.create]] for a pure usage of [[S3]].
    * They both will make sure that the s3 connection is created with the required
    * resources and guarantee that the client was not previously closed.
    *
    * ==Example==
    *
    * {{{
    *   import java.time.Duration
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *
    *   // the exceptions related with concurrency or timeouts from any of the requests
    *   // might be solved by raising the `maxConcurrency`, `maxPendingConnectionAcquire` or
    *   // `connectionAcquisitionTimeout` from the underlying netty http async client.
    *   // see below an example on how to increase such values.
    *
    *   val httpClient = NettyNioAsyncHttpClient.builder()
    *     .maxConcurrency(500)
    *     .maxPendingConnectionAcquires(50000)
    *     .connectionAcquisitionTimeout(Duration.ofSeconds(60))
    *     .readTimeout(Duration.ofSeconds(60))
    *     .build()
    *
    *   val s3AsyncClient: S3AsyncClient = S3AsyncClient
    *     .builder()
    *     .httpClient(httpClient)
    *     .credentialsProvider(DefaultCredentialsProvider.create())
    *     .region(AWS_GLOBAL)
    *     .build()
    *
    *     val s3: S3 = S3.createUnsafe(s3AsyncClient)
    * }}}
    *
    * @param s3AsyncClient an instance of a [[S3AsyncClient]].
    * @return An instance of [[S3]]
    */
  @UnsafeBecauseImpure
  def createUnsafe(s3AsyncClient: S3AsyncClient): S3 = {
    new S3 {
      override val s3Client: S3AsyncClient = s3AsyncClient
    }
  }

  /**
    * Creates a new [[S3]] instance out of the the passed AWS configurations.
    *
    * It provides a fast forward access to the [[S3]] that avoids
    * dealing with [[Resource]], however in this case, the created
    * resources will not be released like in [[create]].
    * Thus, it is the user's responsability to close the [[S3]] connection.
    *
    * ==Example==
    *
    * {{{
    *   import monix.execution.Scheduler.Implicits.global
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region
    *
    *   val defaultCred = DefaultCredentialsProvider.create()
    *   val s3: S3 = S3.createUnsafe(defaultCred, Region.AWS_GLOBAL)
    *   // do your stuff here
    *   s3.close.runToFuture
    * }}}
    *
    * @param credentialsProvider Strategy for loading credentials and authenticate to AWS S3
    * @param region An Amazon Web Services region that hosts a set of Amazon services.
    * @param endpoint The endpoint with which the SDK should communicate.
    * @param httpClient Sets the [[SdkAsyncHttpClient]] that the SDK service client will use to make HTTP calls.
    * @return a [[Resource]] of [[Task]] that allocates and releases [[S3]].
    */
  @UnsafeBecauseImpure
  def createUnsafe(
    credentialsProvider: AwsCredentialsProvider,
    region: Region,
    endpoint: Option[String] = None,
    httpClient: Option[SdkAsyncHttpClient] = None): S3 = {
    val s3AsyncClient = AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
    self.createUnsafe(s3AsyncClient)
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def createBucket(
    bucket: String,
    acl: Option[BucketCannedACL] = None,
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

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def createBucket(request: CreateBucketRequest)(implicit s3AsyncClient: S3AsyncClient): Task[CreateBucketResponse] = {
    Task.from(s3AsyncClient.createBucket(request))
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
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

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def copyObject(request: CopyObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[CopyObjectResponse] = {
    Task.from(s3AsyncClient.copyObject(request))
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def deleteBucket(bucket: String)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteBucketResponse] = {
    Task.from(s3AsyncClient.deleteBucket(S3RequestBuilder.deleteBucket(bucket)))
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def deleteBucket(request: DeleteBucketRequest)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteBucketResponse] = {
    Task.from(s3AsyncClient.deleteBucket(request))
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
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

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def deleteObject(request: DeleteObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[DeleteObjectResponse] =
    Task.from(s3AsyncClient.deleteObject(request))

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def existsBucket(bucket: String)(implicit s3AsyncClient: S3AsyncClient): Task[Boolean] =
    S3.listBuckets().existsL(_.name == bucket)

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
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

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
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

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def download(request: GetObjectRequest)(implicit s3AsyncClient: S3AsyncClient): Task[Array[Byte]] = {
    Task
      .from(s3AsyncClient.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]))
      .map(_.asByteArray())
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def downloadMultipart(
    bucket: String,
    key: String,
    chunkSize: Long = domain.awsMinChunkSize,
    downloadSettings: DownloadSettings = DefaultDownloadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Observable[Array[Byte]] = {
    new MultipartDownloadObservable(bucket, key, chunkSize, downloadSettings, S3.createUnsafe(s3AsyncClient))
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def listBuckets()(implicit s3AsyncClient: S3AsyncClient): Observable[Bucket] = {
    for {
      response <- Observable.fromTaskLike(s3AsyncClient.listBuckets())
      bucket   <- Observable.from(response.buckets().asScala.toList)
    } yield bucket
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def listObjects(
    bucket: String,
    prefix: Option[String] = None,
    maxTotalKeys: Option[Int] = None,
    requestPayer: Option[RequestPayer] = None)(implicit s3AsyncClient: S3AsyncClient): Observable[S3Object] = {
    for {
      listResponse <- ListObjectsObservable(bucket, prefix, maxTotalKeys, requestPayer, s3AsyncClient)
      s3Object     <- Observable.fromIterable(listResponse.contents.asScala)
    } yield s3Object
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def upload(bucket: String, key: String, content: Array[Byte], uploadSettings: UploadSettings = DefaultUploadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Task[PutObjectResponse] = {
    val actualLength: Long = content.length.toLong
    val request: PutObjectRequest =
      S3RequestBuilder.putObjectRequest(bucket, key, Some(actualLength), uploadSettings)
    Task.from(s3AsyncClient.putObject(request, AsyncRequestBody.fromBytes(content)))
  }

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def upload(request: PutObjectRequest, content: Array[Byte])(
    implicit
    s3AsyncClient: S3AsyncClient): Task[PutObjectResponse] =
    Task.from(s3AsyncClient.putObject(request, AsyncRequestBody.fromBytes(content)))

  @deprecated("Use one of the builders like `S3.create`", "0.5.0")
  def uploadMultipart(
    bucket: String,
    key: String,
    minChunkSize: Int = awsMinChunkSize,
    uploadSettings: UploadSettings = DefaultUploadSettings)(
    implicit
    s3AsyncClient: S3AsyncClient): Consumer[Array[Byte], CompleteMultipartUploadResponse] =
    new MultipartUploadSubscriber(bucket, key, minChunkSize, uploadSettings, s3AsyncClient)
}

private[s3] trait S3 { self =>

  private[s3] val s3Client: S3AsyncClient

  /**
    * Creates a bucket.
    *
    * ==Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.s3.S3
    *   import software.amazon.awssdk.services.s3.model.CreateBucketResponse
    *
    *   val bucket = "my-bucket"
    *   val t: Task[CreateBucketResponse] = S3.fromConfig.use(_.createBucket(bucket))
    * }}}
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/CreateBucketRequest.Builder.html
    * @param bucket                     the name of the bucket to be created.
    * @param acl                        the canned ACL (Access Control List) to apply to the object.
    *                                   if the service returns an enum value that is not available in the current SDK version, acl will return ObjectCannedACL.UNKNOWN_TO_SDK_VERSION.
    * @param grantFullControl           gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
    * @param grantRead                  allows grantee to read the object data and its metadata.
    * @param grantReadACP               allows grantee to read the object ACL.
    * @param grantWriteACP              allows grantee to write the ACL for the applicable object.
    * @param objectLockEnabledForBucket specifies whether you want S3 Object Lock to be enabled for the new bucket.
    * @return a [[Task]] with the [[CreateBucketResponse]].
    */
  def createBucket(
    bucket: String,
    acl: Option[BucketCannedACL] = None,
    grantFullControl: Option[String] = None,
    grantRead: Option[String] = None,
    grantReadACP: Option[String] = None,
    grantWrite: Option[String] = None,
    grantWriteACP: Option[String] = None,
    objectLockEnabledForBucket: Option[Boolean] = None): Task[CreateBucketResponse] = {
    Task.from(
      s3Client.createBucket(
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
    * @param request       an instance of [[CreateBucketRequest]]
    * @return a [[Task]] with the create bucket response [[CreateBucketResponse]] .
    */
  def createBucket(request: CreateBucketRequest): Task[CreateBucketResponse] = {
    Task.from(s3Client.createBucket(request))
  }

  /**
    * Creates a copy of from an already stored object.
    *
    * ==Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.s3.S3
    *
    *   val sourceBucket = "source-bucket"
    *   val sourceKey = "source/key.json"
    *   val targetBucket = "target-bucket"
    *   val targetKey = "target/key.json"
    *   val t = S3.fromConfig.use(_.copyObject(sourceBucket, sourceKey, targetBucket, targetKey))
    * }}}
    *
    * @param sourceBucket       the name of the source bucket.
    * @param sourceKey          the key of the source object.
    * @param destinationBucket  the name of the destination bucket.
    * @param destinationKey     the key of the destination object.
    * @param copyObjectSettings adds the [[CopyObjectSettings]] on the request copy object request.
    * @return a [[Task]] containing the result of the CopyObject operation returned by the service.
    */
  def copyObject(
    sourceBucket: String,
    sourceKey: String,
    destinationBucket: String,
    destinationKey: String,
    copyObjectSettings: CopyObjectSettings = DefaultCopyObjectSettings): Task[CopyObjectResponse] = {
    val copyRequest =
      S3RequestBuilder.copyObjectRequest(sourceBucket, sourceKey, destinationBucket, destinationKey, copyObjectSettings)
    self.copyObject(copyRequest)
  }

  /**
    * Creates a copy from an already stored object.
    *
    * @param request       the [[CopyObjectRequest]].
    * @return a [[Task]] containing the result of the CopyObject operation returned by the service.
    */
  def copyObject(request: CopyObjectRequest): Task[CopyObjectResponse] =
    Task.from(s3Client.copyObject(request))

  /**
    * Deletes the specified bucket. Which will only happen when it is empty.
    *
    * @note When attempting to delete a bucket that does not exist, Amazon S3 returns a success message, not an error message.
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteBucketRequest.html
    * @param bucket        the bucket name to be deleted.
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
    * @param request       the AWS delete bucket request of type [[DeleteBucketRequest]]
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
    * @param bucket        the bucket name of the object to be deleted.
    * @param key           the key of the object to be deleted.
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
    S3RequestBuilder.deleteObject(bucket, key, bypassGovernanceRetention, mfa, requestPayer, versionId)
    deleteObject(request)
  }

  /**
    * Deletes the specified object given a [[DeleteBucketRequest]].
    * Once deleted, the object can only be restored if versioning was enabled when the object was deleted.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/DeleteObjectRequest.html
    * @param request       the AWS delete object request of type [[DeleteObjectRequest]]
    * @return a [[Task]] with the delete object response [[DeleteObjectResponse]] .
    */
  def deleteObject(request: DeleteObjectRequest): Task[DeleteObjectResponse] =
    Task.from(s3Client.deleteObject(request))

  /**
    * Check whether the specified bucket exists or not.
    *
    * @param bucket        the bucket name to check its existence
    * @return a boolean [[Task]] indicating whether the bucket exists or not.
    */
  def existsBucket(bucket: String): Task[Boolean] =
    self.listBuckets().existsL(_.name == bucket)

  /**
    * Checks whether the specified object exists or not.
    *
    * ==Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.s3.S3
    *
    *   val bucket = "my-bucket"
    *   val s3Key = "my-key"
    *
    *   val t: Task[Boolean] = S3.fromConfig.use(_.existsObject(bucket, s3Key))
    * }}}
    *
    * @param bucket        the bucket name of the object to check its existence.
    * @param key           the key of the object to be deleted.
    * @return a boolean [[Task]] indicating whether the object existed or not.
    */
  def existsObject(bucket: String, key: String): Task[Boolean] = {
    val headObjectRequest = S3RequestBuilder.headObjectRequest(bucket, Some(key))
    Task
      .from(s3Client.headObject(headObjectRequest))
      .redeemWith(
        ex =>
          if (ex.isInstanceOf[NoSuchKeyException]) falseTask
          else Task.raiseError(ex),
        _ => trueTask)
  }

  private[this] val falseTask = Task.now(false)
  private[this] val trueTask = Task.now(true)

  /**
    * Downloads an object in a single request as byte array.
    *
    * The only two required fields are the [[bucket]] and [[key]], but it also
    * accepts additional settings for more specific requests, see [[DownloadSettings]].
    *
    * WARN! - This method is only suitable for downloading small objects,
    * since it performs a single download request, which might be unsafe
    * when the object is too big to fit in memory or in the http body.
    *
    * @see the safer alternative [[downloadMultipart]] to for downloading objects in parts.
    *
    * ==Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.s3.S3
    *   import cats.effect.Resource
    *
    *   val s3Resource: Resource[Task, S3] = S3.fromConfig // alternatively use `create`
    *
    *   val bucket: String = "sample-bucket"
    *   val key: String = "path/to/test.csv"
    *
    *   // only downloads the first 100 bytes of the object
    *   val t: Task[Array[Byte]] = s3Resource.use(_.download(bucket, key, firstNBytes = Some(100)))
    * }}}
    *
    *  ==Unsafe Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *
    *   // must be properly configured
    *   val s3AsyncClient = S3AsyncClient.builder
    *     .credentialsProvider(DefaultCredentialsProvider.create())
    *     .region(AWS_GLOBAL)
    *     .build()
    *
    *   val s3: S3 = S3.createUnsafe(s3AsyncClient)
    *
    *   // only downloads the first 100 bytes of the object
    *   val arr: Task[Array[Byte]] = s3.download(bucket, key, firstNBytes = Some(100))
    * }}}
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/mediastoredata/model/GetObjectRequest.html
    * @param bucket           target S3 bucket name of the object to be downloaded.
    * @param key              key of the object to be downloaded.
    * @param firstNBytes      downloads the first [[firstNBytes]] from the specified object,
    *                         it must be a positive number if defined.
    * @param downloadSettings additional settings to pass to the download object request.
    * @return a [[Task]] containing the downloaded object as a byte array.
    */
  @Unsafe("OOM risk, use `downloadMultipart` for big downloads.")
  def download(
    bucket: String,
    key: String,
    firstNBytes: Option[Int] = None,
    downloadSettings: DownloadSettings = DefaultDownloadSettings): Task[Array[Byte]] = {
    require(firstNBytes.getOrElse(1) > 0, "The number of bytes if defined, must be positive.")
    val range = firstNBytes.map(n => s"bytes=0-${n - 1}")
    val request: GetObjectRequest = S3RequestBuilder.getObjectRequest(bucket, key, range, downloadSettings)
    Task
      .from(s3Client.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]))
      .map(r => r.asByteArray())
  }

  /**
    * Downloads an object as byte array.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/GetObjectRequest.html
    * @param request     the AWS get object request of type [[GetObjectRequest]].
    * @return A [[Task]] that contains the downloaded object as a byte array.
    */
  def download(request: GetObjectRequest): Task[Array[Byte]] = {
    Task
      .from(s3Client.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]))
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
    *   import monix.eval.Task
    *   import monix.connect.s3.S3
    *   import cats.effect.Resource
    *   import monix.reactive.Consumer
    *   val s3Resource: Resource[Task, S3] = S3.fromConfig
    *   val bucket: String = "sample-bucket"
    *   val key: String = "sample-key"
    *
    *   val t = s3Resource.use { s3 =>
    *     val ob: Observable[Array[Byte]] = s3.downloadMultipart(bucket, key, 2)
    *     // do your business logic
    *     ob.consumeWith(Consumer.complete) // mere sample
    *    }
    * }}}
    *
    * ==Unsafe Example==
    *
    * {{{
    *   import monix.reactive.Observable
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *
    *   // must be properly configured
    *   val client = S3AsyncClient.builder
    *     .credentialsProvider(DefaultCredentialsProvider.create())
    *     .region(AWS_GLOBAL)
    *     .build()
    *
    *   val ob: Observable[Array[Byte]] = S3.createUnsafe(client).downloadMultipart(bucket, key, 2)
    * }}}
    *
    * @param bucket           target S3 bucket name of the object to be downloaded.
    * @param key              path of the object to be downloaded (excluding the bucket).
    * @param chunkSize        amount of bytes to downloaded for each part request,
    *                         must be a positive number, being by default (recommended) 5242880 bytes.
    * @param downloadSettings additional settings to pass to the multipart download request.
    * @return an [[Observable]] that emits chunks of bytes of size [[chunkSize]] until it completes.
    *         in case the object does not exists an [[Array.emptyByteArray]] is returned,
    *         whereas if the bucket does not exists it return a failed [[Task]] of [[software.amazon.awssdk.services.s3.model.NoSuchBucketException]].
    */
  def downloadMultipart(
    bucket: String,
    key: String,
    chunkSize: Long = domain.awsMinChunkSize,
    downloadSettings: DownloadSettings = DefaultDownloadSettings): Observable[Array[Byte]] = {
    require(chunkSize > 0, "Chunk size must be a positive number.")
    new MultipartDownloadObservable(bucket, key, chunkSize, downloadSettings, self)
  }

  /**
    * Lists all of the buckets owned by the authenticated sender of the request.
    *
    * ==Example==
    *
    * {{{
    *   import monix.reactive.Consumer
    *   import monix.reactive.Observable
    *   import monix.eval.Task
    *   import cats.effect.Resource
    *   import software.amazon.awssdk.services.s3.model.Bucket
    *
    *   val s3Resource: Resource[Task, S3] = S3.fromConfig
    *   val bucket: String = "sample-bucket"
    *   val key: String = "sample-key"
    *
    *   val t = s3Resource.use { s3 =>
    *     val buckets: Observable[Bucket] = s3.listBuckets()
    *     // your business logic here
    *     buckets.consumeWith(Consumer.complete) // mere example
    *   }
    * }}}
    *
    * ==Unsafe Example==
    *
    * {{{
    *   import monix.reactive.Observable
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.services.s3.model.Bucket
    *
    *   // must be propelry configured
    *   val s3AsyncClient = S3AsyncClient.builder
    *     .credentialsProvider(DefaultCredentialsProvider.create())
    *     .region(AWS_GLOBAL)
    *     .build()
    *
    *   val buckets: Observable[Bucket] =  S3.createUnsafe(s3AsyncClient).listBuckets()
    * }}}
    *
    * @return an [[Observable]] that emits the list of existing [[Bucket]]s.
    */
  def listBuckets(): Observable[Bucket] = {
    for {
      response <- Observable.fromTaskLike(s3Client.listBuckets())
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
    *   import monix.reactive.Consumer
    *   import monix.reactive.Observable
    *   import monix.eval.Task
    *   import cats.effect.Resource
    *   import software.amazon.awssdk.services.s3.model.S3Object
    *
    *   val s3Resource: Resource[Task, S3] = S3.fromConfig
    *   val bucket = "my-bucket"
    *   val prefix = "prefix/to/list/keys/"
    *
    *   val t = s3Resource.use { s3 =>
    *     val s3Objects: Observable[S3Object] =
    *         s3.listObjects(bucket, maxTotalKeys = Some(1011), prefix = Some(prefix))
    *      //your business logic here
    *      s3Objects.consumeWith(Consumer.complete) //mere example
    *   }
    * }}}
    *
    * ==Unsafe Example==
    *
    * {{{
    *   import monix.reactive.Observable
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.services.s3.model.S3Object
    *
    *   // must be properly configured
    *   val s3AsyncClient = S3AsyncClient.builder
    *     .credentialsProvider(DefaultCredentialsProvider.create())
    *     .region(AWS_GLOBAL)
    *     .build()
    *
    *   val s3: S3 = S3.createUnsafe(s3AsyncClient)
    *
    *   val s3Objects: Observable[S3Object] = s3.listObjects(bucket, maxTotalKeys = Some(1011), prefix = Some(prefix))
    * }}}
    *
    * To use this operation in an AWS (IAM) policy, you must have permissions to perform
    * the `ListBucket` action. The bucket owner has this permission by default and can grant it.
    *
    * @param bucket        target S3 bucket name of the object to be downloaded.
    * @param maxTotalKeys  sets the maximum number of keys to be list,
    *                      it must be a positive number.
    * @param prefix        limits the response to keys that begin with the specified prefix.
    * @param requestPayer  confirms that the requester knows that she or he will be charged for
    *                      the list objects request in V2 style.
    *                      Bucket owners need not specify this parameter in their requests.
    * @return an [[Observable]] that emits the [[S3Object]]s.
    */
  def listObjects(
    bucket: String,
    prefix: Option[String] = None,
    maxTotalKeys: Option[Int] = None,
    requestPayer: Option[RequestPayer] = None): Observable[S3Object] = {
    for {
      listResponse <- ListObjectsObservable(bucket, prefix, maxTotalKeys, requestPayer, this.s3Client)
      s3Object     <- Observable.fromIterable(listResponse.contents.asScala)
    } yield s3Object
  }

  /**
    * Uploads a new object to the specified Amazon S3 bucket.
    *
    * ==Example==
    *
    * {{{
    *   import monix.reactive.Observable
    *   import monix.eval.Task
    *   import cats.effect.Resource
    *
    *   val s3Resource: Resource[Task, S3] = S3.fromConfig
    *   val bucket: String = "sample-bucket"
    *   val key: String = "sample/s3/object"
    *   val content: Array[Byte] = "Whatever your content is".getBytes()
    *
    *   val t = s3Resource.use(_.upload(bucket, key, content))
    * }}}
    *
    * ==Unsafe Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.s3.S3
    *   import software.amazon.awssdk.services.s3.model.PutObjectResponse
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.services.s3.model.S3Object
    *
    *   // must be properly configured
    *   val s3AsyncClient = S3AsyncClient.builder
    *     .credentialsProvider(DefaultCredentialsProvider.create())
    *     .region(AWS_GLOBAL)
    *     .build()
    *
    *   val s3: S3 = S3.createUnsafe(s3AsyncClient)
    *
    *   val response = s3.upload(bucket, key, content)
    * }}}
    *
    * @param bucket  the bucket where this request will upload a new object to
    * @param key     key under which to store the new object
    * @param content text content to be uploaded
    * @return response from the put object http request as [[PutObjectResponse]]
    */
  def upload(
    bucket: String,
    key: String,
    content: Array[Byte],
    uploadSettings: UploadSettings = DefaultUploadSettings): Task[PutObjectResponse] = {
    val actualLength: Long = content.length.toLong
    val request: PutObjectRequest =
      S3RequestBuilder.putObjectRequest(bucket, key, Some(actualLength), uploadSettings)
    Task.from(s3Client.putObject(request, AsyncRequestBody.fromBytes(content)))
  }

  /**
    * Uploads a new object to the specified Amazon S3 bucket.
    *
    * @see https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/model/PutObjectRequest.html
    * @param request       instance of [[PutObjectRequest]]
    * @param content       content to be uploaded
    * @return the response from the http put object request as [[PutObjectResponse]].
    */
  def upload(request: PutObjectRequest, content: Array[Byte]): Task[PutObjectResponse] =
    Task.from(s3Client.putObject(request, AsyncRequestBody.fromBytes(content)))

  /**
    * Uploads an S3 object by making multiple http requests (parts) of the received chunks of bytes.
    *
    * ==Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.reactive.{Observable, Consumer}
    *   import cats.effect.Resource
    *
    *   val s3Resource: Resource[Task, S3] = S3.fromConfig
    *   val bucket: String = "sample-bucket"
    *   val key: String = "sample/key/to/s3/object"
    *   val content: Array[Byte] = "Hello World!".getBytes
    *
    *   val t = s3Resource.use { s3 =>
    *     Observable.pure(content).consumeWith(s3.uploadMultipart(bucket, key))
    *   }
    * }}}
    *
    * ==Unsafe Example==
    *
    * {{{
    *   import monix.eval.Task
    *   import monix.reactive.{Observable, Consumer}
    *   import monix.connect.s3.S3
    *   import software.amazon.awssdk.services.s3.S3AsyncClient
    *   import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *
    *   // must be properly configured
    *   val s3AsyncClient = S3AsyncClient.builder
    *     .credentialsProvider(DefaultCredentialsProvider.create())
    *     .region(AWS_GLOBAL)
    *     .build()
    *
    *   val s3: S3 = S3.createUnsafe(s3AsyncClient)
    *
    *   val response = Observable.pure(content).consumeWith(s3.uploadMultipart(bucket, key))
    * }}}
    *
    * @param bucket        the bucket name where the object will be stored
    * @param key           the key where the object will be stored.
    * @param minChunkSize  size of the chunks (parts) that will be sent in the http body. (the minimum size is set by default, don't use a lower one)
    * @return the confirmation of the multipart whole upload as [[CompleteMultipartUploadResponse]].
    */
  def uploadMultipart(
    bucket: String,
    key: String,
    minChunkSize: Int = awsMinChunkSize,
    uploadSettings: UploadSettings = DefaultUploadSettings): Consumer[Array[Byte], CompleteMultipartUploadResponse] = {
    require(minChunkSize >= domain.awsMinChunkSize, "minChunkSize >= 5242880")
    new MultipartUploadSubscriber(bucket, key, minChunkSize, uploadSettings, s3Client)
  }

  /**
    * Closes the underlying [[S3AsyncClient]].
    */
  def close: Task[Unit] = Task.evalOnce(s3Client.close())

}
