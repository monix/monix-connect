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
  def deleteBucket(bucket: String): DeleteBucketRequest = {
    DeleteBucketRequest
      .builder()
      .bucket(bucket)
      .build()
  }

  /** A builder for [[DeleteObjectRequest]]. */
  def deleteObject(
    bucket: String,
    key: String,
    bypassGovernanceRetention: Option[Boolean] = None,
    mfa: Option[String] = None,
    requestPayer: Option[String] = None,
    versionId: Option[String] = None): DeleteObjectRequest = {
    val request = DeleteObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
    bypassGovernanceRetention.map(request.bypassGovernanceRetention(_))
    mfa.map(request.mfa(_))
    requestPayer.map(request.requestPayer(_))
    versionId.map(request.versionId(_))
    request.build()
  }

  /** A builder for [[CreateBucketRequest]]. */
  def createBucket(
    bucket: String,
    acl: Option[BucketCannedACL] = None,
    grantFullControl: Option[String] = None,
    grantRead: Option[String] = None,
    grantReadACP: Option[String] = None,
    grantWrite: Option[String] = None,
    grantWriteACP: Option[String] = None,
    objectLockEnabledForBucket: Option[Boolean] = None): CreateBucketRequest = {
    val request = CreateBucketRequest
      .builder()
      .bucket(bucket)
    acl.map(request.acl(_))
    grantFullControl.map(request.grantFullControl(_))
    grantRead.map(request.grantRead(_))
    grantReadACP.map(request.grantReadACP(_))
    grantWrite.map(request.grantWrite(_))
    grantWriteACP.map(request.grantWriteACP(_))
    objectLockEnabledForBucket.map(request.objectLockEnabledForBucket(_))
    request.build()
  }

  /** A builder for [[CompletedPart]]. */
  def completedPart(partN: Int, uploadPartResp: UploadPartResponse): CompletedPart =
    CompletedPart
      .builder()
      .partNumber(partN)
      .eTag(uploadPartResp.eTag())
      .build()

  /** A builder for [[CompleteMultipartUploadRequest]]. */
  def completeMultipartUploadRquest(
    bucket: String,
    key: String,
    uploadId: String,
    completedParts: List[CompletedPart],
    requestPayer: Option[RequestPayer]): CompleteMultipartUploadRequest = {
    val completedMultipartUpload = CompletedMultipartUpload.builder.parts(completedParts.asJava).build()
    val request: CompleteMultipartUploadRequest.Builder = CompleteMultipartUploadRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .uploadId(uploadId)
      .multipartUpload(completedMultipartUpload)
    requestPayer.map(request.requestPayer(_))
    request.build()
  }

  /** A builder for [[CreateMultipartUploadRequest]]. */
  def createMultipartUploadRequest(
    bucket: String,
    key: String,
    uploadSettings: UploadSettings): CreateMultipartUploadRequest = {
    val request: CreateMultipartUploadRequest.Builder = CreateMultipartUploadRequest
      .builder()
      .bucket(bucket)
      .key(key)
    //contentType.map(request.contentType(_))
    uploadSettings.acl.map(request.acl(_))
    uploadSettings.grantFullControl.map(request.grantFullControl(_))
    uploadSettings.grantRead.map(request.grantRead(_))
    uploadSettings.grantReadACP.map(request.grantReadACP(_))
    uploadSettings.grantWriteACP.map(request.grantWriteACP(_))
    uploadSettings.requestPayer.map(request.requestPayer(_))
    uploadSettings.serverSideEncryption.map(request.serverSideEncryption(_))
    uploadSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm(_))
    uploadSettings.sseCustomerKey.map(request.sseCustomerKey(_))
    uploadSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5(_))
    uploadSettings.ssekmsEncryptionContext.map(request.ssekmsEncryptionContext(_))
    uploadSettings.ssekmsKeyId.map(request.ssekmsKeyId(_))
    request.build()
  }

  /** A builder for [[CopyObjectRequest]]. */
  def copyObjectRequest(
    sourceBucket: String,
    sourceKey: String,
    destinationBucket: String,
    destinationKey: String,
    copyObjectSettings: CopyObjectSettings) = {
    val request = CopyObjectRequest
      .builder()
      .copySource(sourceBucket + "/" + sourceKey)
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
    request.build()
  }

  /**
    * A builder that requires accepts the minimum required fields ([[bucket]], [[key]]) and some
    * additional settings to build a [[GetObjectRequest]].
    */
  def getObjectRequest(
    bucket: String,
    key: String,
    range: Option[String] = None,
    downloadSettings: DownloadSettings = DefaultDownloadSettings): GetObjectRequest = {
    val request: GetObjectRequest.Builder = GetObjectRequest.builder().bucket(bucket).key(key)
    downloadSettings.ifMatch.map(request.ifMatch(_))
    downloadSettings.ifModifiedSince.map(request.ifModifiedSince(_))
    downloadSettings.ifNoneMatch.map(request.ifNoneMatch(_))
    downloadSettings.ifUnmodifiedSince.map(request.ifUnmodifiedSince(_))
    range.map(request.range(_))
    downloadSettings.versionId.map(request.versionId(_))
    downloadSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm(_))
    downloadSettings.sseCustomerKey.map(request.sseCustomerKey(_))
    downloadSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5(_))
    downloadSettings.requestPayer.map(request.requestPayer(_))
    //partNumber.map(request.partNumber(_)) maybe to add in the future
    request.build()
  }

  /** A builder for [[HeadObjectRequest]]. */
  def headObjectRequest(
    bucket: String,
    key: Option[String],
    ifMatch: Option[String] = None,
    ifModifiedSince: Option[Instant] = None,
    ifEtagMatch: Option[String] = None) = {
    val request = HeadObjectRequest.builder()
    request.bucket(bucket)
    key.map(request.key(_))
    ifMatch.map(request.ifMatch(_))
    ifModifiedSince.map(request.ifModifiedSince(_))
    ifEtagMatch.map(request.ifNoneMatch(_))
    request.build()
  }

  /** A builder for [[ListObjectsV2Request]]. */
  def listObjectsV2(
    bucket: String,
    continuationToken: Option[String] = None,
    fetchOwner: Option[Boolean] = None,
    maxKeys: Option[Int] = None,
    prefix: Option[String] = None,
    startAfter: Option[String] = None,
    requestPayer: Option[RequestPayer] = None): ListObjectsV2Request = {
    val request = ListObjectsV2Request.builder().bucket(bucket)
    fetchOwner.map(request.fetchOwner(_))
    startAfter.map(request.startAfter(_))
    continuationToken.map(request.continuationToken(_))
    maxKeys.map(request.maxKeys(_))
    prefix.map(request.prefix(_))
    requestPayer.map(request.requestPayer(_))
    request.build()
  }

  /** A builder for [[UploadPartRequest]]. */
  def uploadPartRequest(
    bucket: String,
    key: String,
    partN: Int,
    uploadId: String,
    contentLenght: Long,
    uploadSettings: UploadSettings = DefaultUploadSettings): UploadPartRequest = {
    val request =
      UploadPartRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .partNumber(partN)
        .uploadId(uploadId)
        .contentLength(contentLenght)
    uploadSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm(_))
    uploadSettings.sseCustomerKey.map(request.sseCustomerKey(_))
    uploadSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5(_))
    uploadSettings.requestPayer.map(request.requestPayer(_))
    request.build()
  }

  /** Builder for [[PutObjectRequest]]. */
  def putObjectRequest(
    bucket: String,
    key: String,
    contentLenght: Option[Long],
    uploadSettings: UploadSettings = DefaultUploadSettings) = {
    val request = PutObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
    contentLenght.map(request.contentLength(_))
    //contentType.map(request.contentType(_))
    uploadSettings.acl.map(request.acl(_))
    uploadSettings.grantFullControl.map(request.grantFullControl(_))
    uploadSettings.grantRead.map(request.grantRead(_))
    uploadSettings.grantReadACP.map(request.grantReadACP(_))
    uploadSettings.grantWriteACP.map(request.grantWriteACP(_))
    uploadSettings.requestPayer.map(request.requestPayer(_))
    uploadSettings.serverSideEncryption.map(request.serverSideEncryption(_))
    uploadSettings.sseCustomerAlgorithm.map(request.sseCustomerAlgorithm(_))
    uploadSettings.sseCustomerKey.map(request.sseCustomerKey(_))
    uploadSettings.sseCustomerKeyMD5.map(request.sseCustomerKeyMD5(_))
    uploadSettings.ssekmsEncryptionContext.map(request.ssekmsEncryptionContext(_))
    uploadSettings.ssekmsKeyId.map(request.ssekmsKeyId(_))
    request.build()
  }
}
