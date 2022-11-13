/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
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

import java.time.Instant

import monix.connect.s3.domain._
import monix.execution.internal.InternalApi
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters._

/**
  * A class that provides converter methods that given the required set of parameters for that
  * conversion, it builds the relative AWS java object.
  */
@InternalApi
private[s3] object S3RequestBuilder {

  /**
    * A builder for [[DeleteBucketRequest]]
    */
  private[s3] def deleteBucket(bucket: String): DeleteBucketRequest = {
    DeleteBucketRequest.builder
      .bucket(bucket)
      .build
  }

  /** A builder for [[DeleteObjectRequest]]. */
  private[s3] def deleteObject(
    bucket: String,
    key: String,
    bypassGovernanceRetention: Option[Boolean] = None,
    mfa: Option[String] = None,
    requestPayer: Option[String] = None,
    versionId: Option[String] = None): DeleteObjectRequest = {
    val request = DeleteObjectRequest.builder
      .bucket(bucket)
      .key(key)
    bypassGovernanceRetention.map(request.bypassGovernanceRetention(_))
    mfa.map(request.mfa)
    requestPayer.map(request.requestPayer)
    versionId.map(request.versionId)
    request.build
  }

  /** A builder for [[CreateBucketRequest]]. */
  private[s3] def createBucket(
    bucket: String,
    acl: Option[BucketCannedACL] = None,
    grantFullControl: Option[String] = None,
    grantRead: Option[String] = None,
    grantReadACP: Option[String] = None,
    grantWrite: Option[String] = None,
    grantWriteACP: Option[String] = None,
    objectLockEnabledForBucket: Option[Boolean] = None): CreateBucketRequest = {
    val request = CreateBucketRequest.builder
      .bucket(bucket)
    acl.map(request.acl)
    grantFullControl.map(request.grantFullControl)
    grantRead.map(request.grantRead)
    grantReadACP.map(request.grantReadACP)
    grantWrite.map(request.grantWrite)
    grantWriteACP.map(request.grantWriteACP)
    objectLockEnabledForBucket.map(request.objectLockEnabledForBucket(_))
    request.build
  }

  /** A builder for [[CompletedPart]]. */
  private[s3] def completedPart(partN: Int, uploadPartResp: UploadPartResponse): CompletedPart =
    CompletedPart.builder
      .partNumber(partN)
      .eTag(uploadPartResp.eTag)
      .build

  /** A builder for [[CompleteMultipartUploadRequest]]. */
  private[s3] def completeMultipartUploadRequest(
    bucket: String,
    key: String,
    uploadId: String,
    completedParts: List[CompletedPart],
    requestPayer: Option[RequestPayer]): CompleteMultipartUploadRequest = {
    val completedMultipartUpload = CompletedMultipartUpload.builder.parts(completedParts.asJava).build()
    val request: CompleteMultipartUploadRequest.Builder = CompleteMultipartUploadRequest.builder
      .bucket(bucket)
      .key(key)
      .uploadId(uploadId)
      .multipartUpload(completedMultipartUpload)
    requestPayer.map(request.requestPayer)
    request.build
  }

  /** A builder for [[CreateMultipartUploadRequest]]. */
  private[s3] def createMultipartUploadRequest(
    bucket: String,
    key: String,
    uploadSettings: UploadSettings): CreateMultipartUploadRequest = {
    val request: CreateMultipartUploadRequest.Builder = CreateMultipartUploadRequest
      .builder()
      .bucket(bucket)
      .key(key)
    //contentType.map(request.contentType(_))
    uploadSettings.acl.map(request.acl)
    uploadSettings.grantFullControl.map(request.grantFullControl)
    uploadSettings.grantRead.map(request.grantRead)
    uploadSettings.grantReadACP.map(request.grantReadACP)
    uploadSettings.grantWriteACP.map(request.grantWriteACP)
    uploadSettings.requestPayer.map(request.requestPayer)
    uploadSettings.serverSideEncryption.map(request.serverSideEncryption)
    uploadSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm)
    uploadSettings.sseCustomerKey.map(request.sseCustomerKey)
    uploadSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5)
    uploadSettings.ssekmsEncryptionContext.map(request.ssekmsEncryptionContext)
    uploadSettings.ssekmsKeyId.map(request.ssekmsKeyId)
    request.build
  }

  /** A builder for [[CopyObjectRequest]]. */
  private[s3] def copyObjectRequest(
    sourceBucket: String,
    sourceKey: String,
    destinationBucket: String,
    destinationKey: String,
    copyObjectSettings: CopyObjectSettings): CopyObjectRequest = {
    val request = CopyObjectRequest.builder
      .sourceKey(sourceBucket + "/" + sourceKey)
      .destinationBucket(destinationBucket)
      .destinationKey(destinationKey)
    copyObjectSettings.copySourceIfMatches.map(request.copySourceIfMatch)
    copyObjectSettings.copySourceIfNoneMatch.map(request.copySourceIfNoneMatch)
    copyObjectSettings.copyIfModifiedSince.map(request.copySourceIfModifiedSince)
    copyObjectSettings.copyIfUnmodifiedSince.map(request.copySourceIfUnmodifiedSince)
    copyObjectSettings.acl.map(request.acl)
    copyObjectSettings.expires.map(request.expires)
    copyObjectSettings.grantFullControl.map(request.grantFullControl)
    copyObjectSettings.grantRead.map(request.grantRead)
    copyObjectSettings.grantReadACP.map(request.grantReadACP)
    copyObjectSettings.grantWriteACP.map(request.grantWriteACP)
    if (copyObjectSettings.metadata.nonEmpty) request.metadata(copyObjectSettings.metadata.asJava)
    copyObjectSettings.metadataDirective.map(request.metadataDirective)
    copyObjectSettings.taggingDirective.map(request.taggingDirective)
    copyObjectSettings.serverSideEncryption.map(request.serverSideEncryption)
    request.storageClass(copyObjectSettings.storageClass)
    copyObjectSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm)
    copyObjectSettings.sseCustomerKey.map(request.sseCustomerKey)
    copyObjectSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5)
    copyObjectSettings.ssekmsKeyId.map(request.ssekmsKeyId)
    copyObjectSettings.copySourceSSECustomerAlgorithm.map(request.copySourceSSECustomerAlgorithm)
    copyObjectSettings.copySourceSSECustomerKey.map(request.copySourceSSECustomerKey)
    copyObjectSettings.copySourceSSECustomerKeyMD5.map(request.copySourceSSECustomerKeyMD5)
    copyObjectSettings.objectLockRetainUntilDate.map(request.objectLockRetainUntilDate)
    copyObjectSettings.objectLockLegalHoldStatus.map(request.objectLockLegalHoldStatus)
    copyObjectSettings.objectLockMode.map(request.objectLockMode)
    copyObjectSettings.requestPayer.map(request.requestPayer)
    request.build
  }

  /**
    * A builder that requires accepts the minimum required fields (`bucket`, `key`) and some
    * additional settings to build a [[GetObjectRequest]].
    */
  private[s3] def getObjectRequest(
    bucket: String,
    key: String,
    range: Option[String] = None,
    downloadSettings: DownloadSettings = DefaultDownloadSettings): GetObjectRequest = {
    val request: GetObjectRequest.Builder = GetObjectRequest.builder.bucket(bucket).key(key)
    downloadSettings.ifMatch.map(request.ifMatch)
    downloadSettings.ifModifiedSince.map(request.ifModifiedSince)
    downloadSettings.ifNoneMatch.map(request.ifNoneMatch)
    downloadSettings.ifUnmodifiedSince.map(request.ifUnmodifiedSince)
    range.map(request.range)
    downloadSettings.versionId.map(request.versionId)
    downloadSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm)
    downloadSettings.sseCustomerKey.map(request.sseCustomerKey)
    downloadSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5)
    downloadSettings.requestPayer.map(request.requestPayer)
    //partNumber.map(request.partNumber(_)) maybe to add in the future
    request.build
  }

  /** A builder for [[HeadObjectRequest]]. */
  private[s3] def headObjectRequest(
    bucket: String,
    key: Option[String],
    ifMatch: Option[String] = None,
    ifModifiedSince: Option[Instant] = None,
    ifEtagMatch: Option[String] = None): HeadObjectRequest = {
    val request = HeadObjectRequest.builder()
    request.bucket(bucket)
    key.map(request.key)
    ifMatch.map(request.ifMatch)
    ifModifiedSince.map(request.ifModifiedSince)
    ifEtagMatch.map(request.ifNoneMatch)
    request.build()
  }

  /** A builder for [[ListObjectsV2Request]]. */
  private[s3] def listObjectsV2(
    bucket: String,
    continuationToken: Option[String] = None,
    fetchOwner: Option[Boolean] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None,
    startAfter: Option[String] = None,
    requestPayer: Option[RequestPayer] = None): ListObjectsV2Request = {
    val request = ListObjectsV2Request.builder().bucket(bucket)
    fetchOwner.map(request.fetchOwner(_))
    startAfter.map(request.startAfter)
    continuationToken.map(request.continuationToken)
    maxKeys.map(request.maxKeys(_))
    prefix.map(request.prefix)
    requestPayer.map(request.requestPayer)
    request.build()
  }

  /** A builder for [[UploadPartRequest]]. */
  private[s3] def uploadPartRequest(
    bucket: String,
    key: String,
    partN: Int,
    uploadId: String,
    contentLength: Long,
    uploadSettings: UploadSettings = DefaultUploadSettings): UploadPartRequest = {
    val request =
      UploadPartRequest.builder
        .bucket(bucket)
        .key(key)
        .partNumber(partN)
        .uploadId(uploadId)
        .contentLength(contentLength)
    uploadSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm)
    uploadSettings.sseCustomerKey.map(request.sseCustomerKey)
    uploadSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5)
    uploadSettings.requestPayer.map(request.requestPayer)
    request.build
  }

  /** Builder for [[PutObjectRequest]]. */
  private[s3] def putObjectRequest(
    bucket: String,
    key: String,
    contentLength: Option[Long],
    uploadSettings: UploadSettings = DefaultUploadSettings): PutObjectRequest = {
    val request = PutObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
    contentLength.map(request.contentLength(_))
    //contentType.map(request.contentType(_))
    uploadSettings.acl.map(request.acl)
    uploadSettings.grantFullControl.map(request.grantFullControl)
    uploadSettings.grantRead.map(request.grantRead)
    uploadSettings.grantReadACP.map(request.grantReadACP)
    uploadSettings.grantWriteACP.map(request.grantWriteACP)
    uploadSettings.requestPayer.map(request.requestPayer)
    uploadSettings.serverSideEncryption.map(request.serverSideEncryption)
    uploadSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm)
    uploadSettings.sseCustomerKey.map(request.sseCustomerKey)
    uploadSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5)
    uploadSettings.ssekmsEncryptionContext.map(request.ssekmsEncryptionContext)
    uploadSettings.ssekmsKeyId.map(request.ssekmsKeyId)
    request.build
  }
}
