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

/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
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

import monix.connect.s3.domain.{CopyObjectSettings, DownloadSettings, UploadSettings}
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import software.amazon.awssdk.services.s3.model.{
  BucketCannedACL,
  CompleteMultipartUploadRequest,
  CompletedPart,
  CopyObjectRequest,
  CreateBucketRequest,
  CreateMultipartUploadRequest,
  DeleteBucketRequest,
  DeleteObjectRequest,
  GetObjectRequest,
  PutObjectRequest,
  RequestPayer,
  StorageClass,
  UploadPartRequest,
  UploadPartResponse
}

import scala.jdk.CollectionConverters._

class S3RequestBuilderSpec
  extends AnyWordSpecLike with BeforeAndAfterEach with Matchers with BeforeAndAfterAll with S3RequestGenerators {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {}

  s"${S3RequestBuilder} " should {

    s"correctly build `CreateBucketRequest`" in {
      //given
           val  (bucket: String,
            acl: Option[BucketCannedACL],
            grantFullControl: Option[String],
            grantRead: Option[String],
            grantReadACP: Option[String],
            grantWrite: Option[String],
            grantWriteACP: Option[String]) = genCreateBucketParams.sample.get
          //when
          val request: CreateBucketRequest =
            S3RequestBuilder
              .createBucket(bucket, acl, grantFullControl, grantRead, grantReadACP, grantWrite, grantWriteACP)

          //then
          request.bucket shouldBe bucket
          request.acl() shouldBe acl.orNull
          request.grantFullControl shouldBe grantFullControl.orNull
          request.grantRead shouldBe grantRead.orNull
          request.grantReadACP shouldBe grantReadACP.orNull
          request.grantWrite shouldBe grantWrite.orNull
          request.grantWriteACP shouldBe grantWriteACP.orNull
      }
    }

    s"builds `CopyObjectRequest`" in {
      //given
     val (
            sourceBucket: String,
            sourceKey: String,
            destinationBucket: String,
            destinationKey: String,
            copyObjectSettings: CopyObjectSettings) = genCopyObjectParams.sample.get
          //when
          val request: CopyObjectRequest =
            S3RequestBuilder
              .copyObjectRequest(sourceBucket, sourceKey, destinationBucket, destinationKey, copyObjectSettings)

          //then
          request.copySource shouldBe sourceBucket + "/" + sourceKey
          request.destinationBucket shouldBe destinationBucket
          request.destinationKey shouldBe destinationKey
          request.copySourceIfNoneMatch shouldBe copyObjectSettings.copySourceIfNoneMatch.orNull
          request.copySourceIfModifiedSince shouldBe copyObjectSettings.copyIfModifiedSince.orNull
          request.copySourceIfUnmodifiedSince shouldBe copyObjectSettings.copyIfUnmodifiedSince.orNull
          request.expires shouldBe copyObjectSettings.expires.orNull
          request.acl shouldBe copyObjectSettings.acl.orNull
          request.grantFullControl shouldBe copyObjectSettings.grantFullControl.orNull
          request.grantRead shouldBe copyObjectSettings.grantRead.orNull
          request.grantReadACP shouldBe copyObjectSettings.grantReadACP.orNull
          request.grantWriteACP shouldBe copyObjectSettings.grantWriteACP.orNull
          request.metadata shouldBe Map.empty.asJava
          request.metadataDirective shouldBe copyObjectSettings.metadataDirective.orNull
          request.taggingDirective shouldBe copyObjectSettings.taggingDirective.orNull
          request.serverSideEncryption shouldBe copyObjectSettings.serverSideEncryption.orNull
          request.storageClass shouldBe StorageClass.STANDARD
          request.sseCustomerAlgorithm shouldBe copyObjectSettings.sseCustomerAlgorithm.orNull
          request.sseCustomerKey shouldBe copyObjectSettings.sseCustomerKey.orNull
          request.sseCustomerKeyMD5 shouldBe copyObjectSettings.sseCustomerKeyMD5.orNull
          request.ssekmsKeyId shouldBe copyObjectSettings.ssekmsKeyId.orNull
          request.copySourceSSECustomerAlgorithm shouldBe copyObjectSettings.copySourceSSECustomerAlgorithm.orNull
          request.copySourceSSECustomerKey shouldBe copyObjectSettings.copySourceSSECustomerKey.orNull
          request.copySourceSSECustomerKeyMD5 shouldBe copyObjectSettings.copySourceSSECustomerKeyMD5.orNull
          request.objectLockMode shouldBe copyObjectSettings.objectLockMode.orNull
          request.objectLockRetainUntilDate shouldBe copyObjectSettings.objectLockRetainUntilDate.orNull
          request.objectLockLegalHoldStatus shouldBe copyObjectSettings.objectLockLegalHoldStatus.orNull
          request.requestPayer shouldBe copyObjectSettings.requestPayer.orNull
    }

    s"correctly build `CompletedPart`" in {
      //given
      val partN = Gen.choose(1, 100).sample.get
      val etag = Gen.alphaLowerStr.sample.get

      //when
      val uploadPartResp = UploadPartResponse.builder().eTag(etag).build()
      val request: CompletedPart =
        S3RequestBuilder
          .completedPart(partN, uploadPartResp)

      //then
      request.partNumber shouldBe partN
      request.eTag shouldBe etag
    }

    s"correctly build `CompleteMultipartUploadRequest`" in {
        val (
            bucket: String,
            key: String,
            uploadId: String,
            completedParts: List[CompletedPart],
            requestPayer: Option[RequestPayer]) = genCompleteMultipartUploadParams.sample.get
          //when
          val request: CompleteMultipartUploadRequest =
            S3RequestBuilder
              .completeMultipartUploadRequest(bucket, key, uploadId, completedParts, requestPayer)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.uploadId shouldBe uploadId
          request.multipartUpload.parts shouldBe completedParts.asJava
          request.requestPayer() shouldBe requestPayer.orNull
    }

    s"correctly build `CreateMultipartUploadRequest`" in {
        val (bucket: String, key: String, uploadSettings: UploadSettings) = genCreateMultipartUploadParams.sample.get
          //when
          val request: CreateMultipartUploadRequest =
            S3RequestBuilder
              .createMultipartUploadRequest(bucket, key, uploadSettings)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.acl shouldBe uploadSettings.acl.orNull
          request.grantFullControl shouldBe uploadSettings.grantFullControl.orNull
          request.grantRead shouldBe uploadSettings.grantRead.orNull
          request.grantReadACP shouldBe uploadSettings.grantReadACP.orNull
          request.grantWriteACP shouldBe uploadSettings.grantWriteACP.orNull
          request.requestPayer shouldBe uploadSettings.requestPayer.orNull
          request.serverSideEncryptionAsString shouldBe uploadSettings.serverSideEncryption.orNull
          request.sseCustomerAlgorithm shouldBe uploadSettings.sseCustomerAlgorithm.orNull
          request.sseCustomerKey shouldBe uploadSettings.sseCustomerKey.orNull
          request.sseCustomerKeyMD5 shouldBe uploadSettings.sseCustomerKeyMD5.orNull
          request.ssekmsEncryptionContext shouldBe uploadSettings.ssekmsEncryptionContext.orNull
    }

    s"correctly build `DeleteBucketRequest`s" in {
      //given
      val bucket: String = Gen.alphaLowerStr.sample.get

      //when
      val request: DeleteBucketRequest = S3RequestBuilder.deleteBucket(bucket)

      //then
      request.bucket() shouldBe bucket
    }

    s"correctly build `DeleteObjectRequest`s" in {
      //given
        val (
            bucket: String,
            key: String,
            bypassGovernanceRetention: Option[Boolean],
            mfa: Option[String],
            requestPayer: Option[String],
            versionId: Option[String]) = genDeleteObjectParams.sample.get
          //when
          val request: DeleteObjectRequest =
            S3RequestBuilder
              .deleteObject(bucket, key, bypassGovernanceRetention, mfa, requestPayer, versionId)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.bypassGovernanceRetention shouldBe bypassGovernanceRetention.getOrElse(null)
          request.mfa shouldBe mfa.orNull
          request.requestPayerAsString shouldBe requestPayer.orNull
          request.versionId shouldBe versionId.orNull
    }

    s"correctly build `GetObjectRequest`s" in {
      //given
      val (bucket: String, key: String, nBytes: Option[String], downloadSettings: DownloadSettings) = genGetObjectParams.sample.get

      //when
          val request: GetObjectRequest =
            S3RequestBuilder
              .getObjectRequest(bucket, key, nBytes, downloadSettings)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.ifMatch shouldBe downloadSettings.ifMatch.orNull
          request.ifModifiedSince shouldBe downloadSettings.ifModifiedSince.orNull
          request.ifNoneMatch shouldBe downloadSettings.ifNoneMatch.orNull
          request.ifUnmodifiedSince shouldBe downloadSettings.ifUnmodifiedSince.orNull
          request.range shouldBe nBytes.orNull
          request.requestPayer() shouldBe downloadSettings.requestPayer.orNull
          request.sseCustomerAlgorithm shouldBe downloadSettings.sseCustomerAlgorithm.orNull
          request.sseCustomerKey shouldBe downloadSettings.sseCustomerKey.orNull
          request.sseCustomerKeyMD5 shouldBe downloadSettings.sseCustomerKeyMD5.orNull
          request.versionId shouldBe downloadSettings.versionId.orNull
    }

    s"correctly build `UploadPartRequest`" in {
      //given
        val (
            bucket: String,
            key: String,
            partN: Int,
            uploadId: String,
            contentLenght: Long,
            uploadSettings: UploadSettings
            ) = genUploadPartParams.sample.get
          //when
          val request: UploadPartRequest =
            S3RequestBuilder
              .uploadPartRequest(bucket, key, partN, uploadId, contentLenght, uploadSettings)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.partNumber shouldBe partN
          request.uploadId shouldBe uploadId
          request.contentLength shouldBe contentLenght
          request.requestPayer() shouldBe uploadSettings.requestPayer.orNull
          request.sseCustomerAlgorithm shouldBe uploadSettings.sseCustomerAlgorithm.orNull
          request.sseCustomerKey shouldBe uploadSettings.sseCustomerKey.orNull
          request.sseCustomerKeyMD5 shouldBe uploadSettings.sseCustomerKeyMD5.orNull
    }

    s"correctly build `PutObjectRequest`" in {
        val (bucket: String, key: String, contentLenght: Option[Long], uploadSettings: UploadSettings) = genPutObjectParams.sample.get
          //when
          val request: PutObjectRequest =
            S3RequestBuilder
              .putObjectRequest(bucket, key, contentLenght, uploadSettings)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.contentLength shouldBe contentLenght.getOrElse(null)
          request.acl shouldBe uploadSettings.acl.orNull
          request.grantFullControl shouldBe uploadSettings.grantFullControl.orNull
          request.grantRead shouldBe uploadSettings.grantRead.orNull
          request.grantReadACP shouldBe uploadSettings.grantReadACP.orNull
          request.grantWriteACP shouldBe uploadSettings.grantWriteACP.orNull
          request.requestPayer shouldBe uploadSettings.requestPayer.orNull
          request.serverSideEncryptionAsString shouldBe uploadSettings.serverSideEncryption.orNull
          request.sseCustomerAlgorithm shouldBe uploadSettings.sseCustomerAlgorithm.orNull
          request.sseCustomerKey shouldBe uploadSettings.sseCustomerKey.orNull
          request.sseCustomerKeyMD5 shouldBe uploadSettings.sseCustomerKeyMD5.orNull
          request.ssekmsEncryptionContext shouldBe uploadSettings.ssekmsEncryptionContext.orNull
  }
}
