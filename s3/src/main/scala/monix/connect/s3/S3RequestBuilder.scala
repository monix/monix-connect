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

import java.time.Instant

import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadRequest,
  CompletedMultipartUpload,
  CompletedPart,
  CreateBucketRequest,
  CreateMultipartUploadRequest,
  DeleteBucketRequest,
  DeleteObjectRequest,
  GetObjectRequest,
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
    * A builder for [[DeleteBucketRequest]]
    */
  def deleteBucket(bucket: String): DeleteBucketRequest = {
    DeleteBucketRequest
      .builder()
      .bucket(bucket)
      .build()
  }

  /**
    * A builder for [[DeleteObjectRequest]]
    */
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

  /**
    * A builder for [[CreateBucketRequest]]
    */
  def createBucket(
                    bucket: String,
                    acl: Option[String] = None,
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

  /**
    * A builder for [[CompletedPart]]
    */
  def completedPart(partN: Int, uploadPartResp: UploadPartResponse): CompletedPart =
    CompletedPart
      .builder()
      .partNumber(partN)
      .eTag(uploadPartResp.eTag())
      .build()

  /**
    * A builder for [[CompleteMultipartUploadRequest]]
    */
  def completeMultipartUploadRquest(
                                     bucket: String,
                                     key: String,
                                     uploadId: String,
                                     completedParts: List[CompletedPart],
                                     requestPayer: Option[String]): CompleteMultipartUploadRequest = {
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

  /**
    * A builder for [[CreateMultipartUploadRequest]]
    */
  def createMultipartUploadRequest(
                                    bucket: String,
                                    key: String,
                                    contentType: Option[String] = None,
                                    acl: Option[String] = None,
                                    grantFullControl: Option[String] = None,
                                    grantRead: Option[String] = None,
                                    grantReadACP: Option[String] = None,
                                    grantWriteACP: Option[String] = None,
                                    requestPayer: Option[String] = None,
                                    serverSideEncryption: Option[String] = None,
                                    sseCustomerAlgorithm: Option[String] = None,
                                    sseCustomerKey: Option[String] = None,
                                    sseCustomerKeyMD5: Option[String] = None,
                                    ssekmsEncryptionContext: Option[String] = None,
                                    ssekmsKeyId: Option[String] = None): CreateMultipartUploadRequest = {
    val request: CreateMultipartUploadRequest.Builder = CreateMultipartUploadRequest
      .builder()
      .bucket(bucket)
      .key(key)
      .contentType(contentType.getOrElse("plain/text"))
    acl.map(request.acl(_))
    grantFullControl.map(request.grantFullControl(_))
    grantRead.map(request.grantRead(_))
    grantReadACP.map(request.grantReadACP(_))
    grantWriteACP.map(request.grantWriteACP(_))
    requestPayer.map(request.requestPayer(_))
    serverSideEncryption.map(request.serverSideEncryption(_))
    sseCustomerAlgorithm.map(request.sseCustomerAlgorithm(_))
    sseCustomerKey.map(request.sseCustomerKey(_))
    sseCustomerKeyMD5.map(request.sseCustomerKeyMD5(_))
    ssekmsEncryptionContext.map(request.ssekmsEncryptionContext(_))
    ssekmsKeyId.map(request.ssekmsKeyId(_))
    request.build()
  }

  /**
    * A builder that only accepts the minimum required fields to build a [[GetObjectRequest]].
    *
    * @param bucket The S3 bucket name
    * @param key The location of the object in s3.
    * @return An instance of [[GetObjectRequest]]
    */
  def getObjectRequest(
    bucket: String,
    key: String,
    ifMatch: Option[String] = None,
    ifModifiedSince: Option[Instant] = None,
    ifNoneMatch: Option[String] = None,
    ifUnmodifiedSince: Option[Instant] = None,
    partNumber: Option[Int] = None,
    range: Option[String] = None,
    requestPayer: Option[String] = None,
    sseCustomerAlgorithm: Option[String] = None,
    sseCustomerKey: Option[String] = None,
    sseCustomerKeyMD5: Option[String] = None,
    versionId: Option[String] = None): GetObjectRequest = {
    val request: GetObjectRequest.Builder = GetObjectRequest.builder().bucket(bucket).key(key)
    ifMatch.map(request.ifMatch(_))
    ifModifiedSince.map(request.ifModifiedSince(_))
    ifNoneMatch.map(request.ifNoneMatch(_))
    ifUnmodifiedSince.map(request.ifUnmodifiedSince(_))
    range.map(request.range(_))
    versionId.map(request.versionId(_))
    sseCustomerAlgorithm.map(request.sseCustomerAlgorithm(_))
    sseCustomerKey.map(request.sseCustomerKey(_))
    sseCustomerKeyMD5.map(request.sseCustomerKeyMD5(_))
    requestPayer.map(request.requestPayer(_))
    partNumber.map(request.partNumber(_))
    request.build()
  }

  /**
    * A builder for [[ListObjectsRequest]]
    */
  def listObjects(
                  bucket: String,
                  marker: Option[String] = None,
                  maxKeys: Option[Int] = None,
                  prefix: Option[String] = None,
                  requestPayer: Option[String] = None): ListObjectsRequest = {
    val request = ListObjectsRequest
      .builder()
      .bucket(bucket)
    prefix.map(request.prefix(_))
    marker.map(request.marker(_))
    maxKeys.map(request.maxKeys(_))
    requestPayer.map(request.requestPayer(_))
    request.build()
  }

  /**
    * A builder for [[ListObjectsV2Request]]
    */
  def listObjectsV2(
                    bucket: String,
                    continuationToken: Option[String] = None,
                    fetchOwner: Option[Boolean] = None,
                    maxKeys: Option[Int] = None,
                    prefix: Option[String] = None,
                    startAfter: Option[String] = None,
                    requestPayer: Option[String] = None): ListObjectsV2Request = {
    val request = ListObjectsV2Request.builder().bucket(bucket)
    fetchOwner.map(request.fetchOwner(_))
    startAfter.map(request.startAfter(_))
    continuationToken.map(request.continuationToken(_))
    maxKeys.map(request.maxKeys(_))
    prefix.map(request.prefix(_))
    requestPayer.map(request.requestPayer(_))
    request.build()
  }

  /**
    * A builder for [[UploadPartRequest]]
    */
  def uploadPartRequest(
    bucket: String,
    key: String,
    partN: Int,
    uploadId: String,
    contentLenght: Long,
    requestPayer: Option[String] = None,
    sseCustomerAlgorithm: Option[String] = None,
    sseCustomerKey: Option[String] = None,
    sseCustomerKeyMD5: Option[String] = None,
    ): UploadPartRequest = {
    val request =
      UploadPartRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .partNumber(partN)
        .uploadId(uploadId)
        .contentLength(contentLenght) //todo checK optionality
    sseCustomerAlgorithm.map(request.sseCustomerAlgorithm(_))
    sseCustomerKey.map(request.sseCustomerKey(_))
    sseCustomerKeyMD5.map(request.sseCustomerKeyMD5(_))
    requestPayer.map(request.requestPayer(_))
    request.build()
  }

  /**
    * A builder for [[PutObjectRequest]]
    */
  def putObjectRequest(
    bucket: String,
    key: String,
    contentLenght: Long, //todo check optionallity
    contentType: Option[String] = None,
    acl: Option[String],
    grantFullControl: Option[String],
    grantRead: Option[String],
    grantReadACP: Option[String],
    grantWriteACP: Option[String],
    requestPayer: Option[String],
    serverSideEncryption: Option[String],
    sseCustomerAlgorithm: Option[String],
    sseCustomerKey: Option[String],
    sseCustomerKeyMD5: Option[String],
    ssekmsEncryptionContext: Option[String],
    ssekmsKeyId: Option[String]) = {
    val request = PutObjectRequest
      .builder()
      .bucket(bucket)
      .key(key)
    request.contentLength(contentLenght) //todo
    request.contentType(contentType.getOrElse("plain/text")) //todo check optionality
    acl.map(request.acl(_))
    grantFullControl.map(request.grantFullControl(_))
    grantRead.map(request.grantRead(_))
    grantReadACP.map(request.grantReadACP(_))
    grantWriteACP.map(request.grantWriteACP(_))
    requestPayer.map(request.requestPayer(_))
    serverSideEncryption.map(request.serverSideEncryption(_))
    sseCustomerAlgorithm.map(request.sseCustomerAlgorithm(_))
    sseCustomerKey.map(request.sseCustomerKey(_))
    sseCustomerKeyMD5.map(request.sseCustomerKeyMD5(_))
    ssekmsEncryptionContext.map(request.ssekmsEncryptionContext(_))
    ssekmsKeyId.map(request.ssekmsKeyId(_))
    request.build()
  }
}
