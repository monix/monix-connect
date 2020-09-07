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

import monix.connect.s3.domain.{DownloadSettings, UploadSettings}
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import software.amazon.awssdk.services.s3.model.{
  CompleteMultipartUploadRequest,
  CompletedPart,
  CreateBucketRequest,
  CreateMultipartUploadRequest,
  DeleteBucketRequest,
  DeleteObjectRequest,
  GetObjectRequest,
  ListObjectsRequest,
  ListObjectsV2Request,
  PutObjectRequest,
  RequestPayer,
  UploadPartRequest,
  UploadPartResponse
}

import scala.collection.JavaConverters._

class S3RequestBuilderSpec
  extends AnyWordSpecLike with BeforeAndAfterEach with Matchers with BeforeAndAfterAll
  with ScalaCheckDrivenPropertyChecks with S3RequestGenerators {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {}

  s"${S3RequestBuilder} " should {

    s"correctly build `CreateBucketRequest`" in {
      //given
      forAll(genCreateBucketParams) {
        case (
            bucket: String,
            acl: Option[String],
            grantFullControl: Option[String],
            grantRead: Option[String],
            grantReadACP: Option[String],
            grantWrite: Option[String],
            grantWriteACP: Option[String]) =>
          //when
          val request: CreateBucketRequest =
            S3RequestBuilder
              .createBucket(bucket, acl, grantFullControl, grantRead, grantReadACP, grantWrite, grantWriteACP)

          //then
          request.bucket shouldBe bucket
          request.aclAsString shouldBe acl.orNull
          request.grantFullControl shouldBe grantFullControl.orNull
          request.grantRead shouldBe grantRead.orNull
          request.grantReadACP shouldBe grantReadACP.orNull
          request.grantWrite shouldBe grantWrite.orNull
          request.grantWriteACP shouldBe grantWriteACP.orNull
      }
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
      forAll(genCompleteMultipartUploadParams) {
        case (
            bucket: String,
            key: String,
            uploadId: String,
            completedParts: List[CompletedPart],
            requestPayer: Option[RequestPayer]) =>
          //when
          val request: CompleteMultipartUploadRequest =
            S3RequestBuilder
              .completeMultipartUploadRquest(bucket, key, uploadId, completedParts, requestPayer)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.uploadId shouldBe uploadId
          request.multipartUpload.parts shouldBe completedParts.asJava
          request.requestPayer() shouldBe requestPayer.orNull
      }
    }

    s"correctly build `CreateMultipartUploadRequest`" in {
      forAll(genCreateMultipartUploadParams) {
        case (bucket: String, key: String, uploadSettings: UploadSettings) =>
          //when
          val request: CreateMultipartUploadRequest =
            S3RequestBuilder
              .createMultipartUploadRequest(bucket, key, uploadSettings)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.aclAsString shouldBe uploadSettings.acl.orNull
          request.grantFullControl shouldBe uploadSettings.grantFullControl.orNull
          request.grantRead shouldBe uploadSettings.grantRead.orNull
          request.grantReadACP shouldBe uploadSettings.grantReadACP.orNull
          request.grantWriteACP shouldBe uploadSettings.grantWriteACP.orNull
          request.requestPayer() shouldBe uploadSettings.requestPayer.orNull
          request.serverSideEncryptionAsString shouldBe uploadSettings.serverSideEncryption.orNull
          request.sseCustomerAlgorithm shouldBe uploadSettings.sseCustomerAlgorithm.orNull
          request.sseCustomerKey shouldBe uploadSettings.sseCustomerKey.orNull
          request.sseCustomerKeyMD5 shouldBe uploadSettings.sseCustomerKeyMD5.orNull
          request.ssekmsEncryptionContext shouldBe uploadSettings.ssekmsEncryptionContext.orNull
      }
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
      forAll(genDeleteObjectParams) {
        case (
            bucket: String,
            key: String,
            bypassGovernanceRetention: Option[Boolean],
            mfa: Option[String],
            requestPayer: Option[String],
            versionId: Option[String]) =>
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
    }

    s"correctly build `GetObjectRequest`s" in {
      //given
      forAll(genGetObjectParams) {
        case (bucket: String, key: String, nBytes: Option[String], downloadSettings: DownloadSettings) =>
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
    }

    s"correctly build `UploadPartRequest`" in {
      //given
      forAll(genUploadPartParams) {
        case (
            bucket: String,
            key: String,
            partN: Int,
            uploadId: String,
            contentLenght: Long,
            uploadSettings: UploadSettings
            ) =>
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
    }

    s"correctly build `PutObjectRequest`" in {
      forAll(genPutObjectParams) {
        case (bucket: String, key: String, contentLenght: Option[Long], uploadSettings: UploadSettings) =>
          //when
          val request: PutObjectRequest =
            S3RequestBuilder
              .putObjectRequest(bucket, key, contentLenght, uploadSettings)

          //then
          request.bucket shouldBe bucket
          request.key shouldBe key
          request.contentLength shouldBe contentLenght.getOrElse(null)
          request.aclAsString shouldBe uploadSettings.acl.orNull
          request.grantFullControl shouldBe uploadSettings.grantFullControl.orNull
          request.grantRead shouldBe uploadSettings.grantRead.orNull
          request.grantReadACP shouldBe uploadSettings.grantReadACP.orNull
          request.grantWriteACP shouldBe uploadSettings.grantWriteACP.orNull
          request.requestPayer() shouldBe uploadSettings.requestPayer.orNull
          request.serverSideEncryptionAsString shouldBe uploadSettings.serverSideEncryption.orNull
          request.sseCustomerAlgorithm shouldBe uploadSettings.sseCustomerAlgorithm.orNull
          request.sseCustomerKey shouldBe uploadSettings.sseCustomerKey.orNull
          request.sseCustomerKeyMD5 shouldBe uploadSettings.sseCustomerKeyMD5.orNull
          request.ssekmsEncryptionContext shouldBe uploadSettings.ssekmsEncryptionContext.orNull
      }
    }
  }
}
