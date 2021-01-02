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

import monix.connect.s3.domain.{CopyObjectSettings, DownloadSettings, UploadSettings}
import org.scalacheck.Gen
import software.amazon.awssdk.services.s3.model.{
  BucketCannedACL,
  CompletedPart,
  MetadataDirective,
  ObjectCannedACL,
  ObjectLockLegalHoldStatus,
  ObjectLockMode,
  RequestPayer,
  ServerSideEncryption,
  StorageClass,
  TaggingDirective
}

trait S3RequestGenerators {

  val genOptionStr = Gen.option(Gen.alphaLowerStr)
  val genOptionBool = Gen.option(Gen.oneOf(true, false))
  val genRequestPayer: Gen[Option[RequestPayer]] =
    Gen.option(Gen.oneOf(RequestPayer.REQUESTER, RequestPayer.UNKNOWN_TO_SDK_VERSION))
  val genObjectLockLegalHoldStatus: Gen[Option[ObjectLockLegalHoldStatus]] =
    Gen.option(Gen.oneOf(ObjectLockLegalHoldStatus.ON, ObjectLockLegalHoldStatus.OFF))
  val genInstant: Gen[Option[Instant]] = Gen.option(Instant.now())
  val genObjectLockMode: Gen[Option[ObjectLockMode]] =
    Gen.option(Gen.oneOf(ObjectLockMode.COMPLIANCE, ObjectLockMode.GOVERNANCE))
  val genMetadataDirective: Gen[Option[MetadataDirective]] =
    Gen.option(Gen.oneOf(MetadataDirective.COPY, MetadataDirective.REPLACE))
  val genTaggingDirective: Gen[Option[TaggingDirective]] =
    Gen.option(Gen.oneOf(TaggingDirective.COPY, TaggingDirective.REPLACE))
  val genServerSideEncription: Gen[Option[ServerSideEncryption]] =
    Gen.option(Gen.oneOf(ServerSideEncryption.AES256, ServerSideEncryption.AWS_KMS))
  val genObjectAcl = Gen.option(Gen.oneOf(ObjectCannedACL.AUTHENTICATED_READ, ObjectCannedACL.PUBLIC_READ_WRITE))
  val genBucketAcl: Gen[Option[BucketCannedACL]] =
    Gen.option(Gen.oneOf(BucketCannedACL.PRIVATE, BucketCannedACL.PUBLIC_READ_WRITE))

  val genCreateBucketParams = for {
    bucket           <- Gen.alphaLowerStr
    acl              <- genBucketAcl
    grantFullControl <- genOptionStr
    grantRead        <- genOptionStr
    grantReadACP     <- genOptionStr
    grantWrite       <- genOptionStr
    grantWriteACP    <- genOptionStr
  } yield (bucket, acl, grantFullControl, grantRead, grantReadACP, grantWrite, grantWriteACP)

  val genCopyObjectSettings = for {
    copySourceIfMatches            <- genOptionStr
    copySourceIfNoneMatch          <- genOptionStr
    copyIfModifiedSince            <- genInstant
    copyIfUnmodifiedSince          <- genInstant
    expires                        <- genInstant
    acl                            <- genObjectAcl
    grantFullControl               <- genOptionStr
    grantRead                      <- genOptionStr
    grantReadACP                   <- genOptionStr
    grantWriteACP                  <- genOptionStr
    metadataDirective              <- genMetadataDirective
    taggingDirective               <- genTaggingDirective
    serverSideEncryption           <- genServerSideEncription
    sseCustomerAlgorithm           <- genOptionStr
    sseCustomerKey                 <- genOptionStr
    sseCustomerKeyMD5              <- genOptionStr
    ssekmsKeyId                    <- genOptionStr
    copySourceSSECustomerAlgorithm <- genOptionStr
    copySourceSSECustomerKey       <- genOptionStr
    copySourceSSECustomerKeyMD5    <- genOptionStr
    objectLockMode                 <- genObjectLockMode
    objectLockRetainUntilDate      <- genInstant
    objectLockLegalHoldStatus      <- genObjectLockLegalHoldStatus
    requestPayer                   <- genRequestPayer
  } yield CopyObjectSettings(
    copySourceIfMatches,
    copySourceIfNoneMatch,
    copyIfModifiedSince,
    copyIfUnmodifiedSince,
    expires,
    acl,
    grantFullControl,
    grantRead,
    grantReadACP,
    grantWriteACP,
    Map.empty,
    metadataDirective,
    taggingDirective,
    serverSideEncryption,
    StorageClass.STANDARD,
    sseCustomerAlgorithm,
    sseCustomerKey,
    sseCustomerKeyMD5,
    ssekmsKeyId,
    copySourceSSECustomerAlgorithm,
    copySourceSSECustomerKey,
    copySourceSSECustomerKeyMD5,
    objectLockMode,
    objectLockRetainUntilDate,
    objectLockLegalHoldStatus,
    requestPayer
  )

  val genCopyObjectBucketParams = for {
    bucket           <- Gen.alphaLowerStr
    acl              <- genOptionStr
    grantFullControl <- genOptionStr
    grantRead        <- genOptionStr
    grantReadACP     <- genOptionStr
    grantWrite       <- genOptionStr
    grantWriteACP    <- genOptionStr
  } yield (bucket, acl, grantFullControl, grantRead, grantReadACP, grantWrite, grantWriteACP)

  val genCopyObjectParams = genCopyObjectSettings.map(settings =>
    ("sourceBucket", "sourceKey", "destinationBucket", "destinationKey", settings))

  val genCompletedPartParams = for {
    partN <- Gen.choose(1, 100)
    eTag  <- Gen.alphaLowerStr
  } yield (partN, eTag)

  val genCompletedPart = for {
    partN <- Gen.choose(1, 1000)
    eTag  <- Gen.alphaLowerStr
  } yield CompletedPart.builder().partNumber(partN).eTag(eTag).build()

  val genCompleteMultipartUploadParams = for {
    bucket         <- Gen.alphaLowerStr
    key            <- Gen.alphaLowerStr
    uploadId       <- Gen.alphaLowerStr
    completedParts <- Gen.listOfN(1, genCompletedPart)
    requestPayer   <- genRequestPayer
  } yield (bucket, key, uploadId, completedParts, requestPayer)

  val genUploadSettings = for {
    acl                     <- genObjectAcl
    grantFullControl        <- genOptionStr
    grantRead               <- genOptionStr
    grantReadACP            <- genOptionStr
    grantWriteACP           <- genOptionStr
    requestPayer            <- genRequestPayer
    serverSideEncryption    <- genOptionStr
    sseCustomerAlgorithm    <- genOptionStr
    sseCustomerKey          <- genOptionStr
    sseCustomerKeyMD5       <- genOptionStr
    ssekmsEncryptionContext <- genOptionStr
    ssekmsKeyId             <- genOptionStr
  } yield {
    UploadSettings(
      acl,
      grantFullControl,
      grantRead,
      grantReadACP,
      grantWriteACP,
      serverSideEncryption,
      sseCustomerAlgorithm,
      sseCustomerKey,
      sseCustomerKeyMD5,
      ssekmsEncryptionContext,
      ssekmsKeyId,
      requestPayer
    )
  }

  val genCreateMultipartUploadParams = for {
    bucket         <- Gen.alphaLowerStr
    key            <- Gen.alphaLowerStr
    uploadSettings <- genUploadSettings
  } yield (bucket, key, uploadSettings)

  val genDeleteObjectParams = for {
    bucket                    <- Gen.alphaLowerStr
    key                       <- Gen.alphaLowerStr
    bypassGovernanceRetention <- genOptionBool
    mfa                       <- genOptionStr
    requestPayer              <- genOptionStr
    versionId                 <- genOptionStr
  } yield (bucket, key, bypassGovernanceRetention, mfa, requestPayer, versionId)

  val genDownloadSettings = for {
    ifMatch              <- genOptionStr
    ifModifiedSince      <- Gen.option(Gen.oneOf(Seq(Instant.now())))
    ifNoneMatch          <- genOptionStr
    ifUnmodifiedSince    <- Gen.option(Gen.oneOf(Seq(Instant.now())))
    partNumber           <- Gen.option(Gen.chooseNum[Int](1, 200)) //maybe to be added in the future
    requestPayer         <- Gen.option(RequestPayer.fromValue("unknown"))
    sseCustomerAlgorithm <- genOptionStr
    sseCustomerKey       <- genOptionStr
    sseCustomerKeyMD5    <- genOptionStr
    versionId            <- genOptionStr
  } yield {
    DownloadSettings(
      ifMatch,
      ifModifiedSince,
      ifNoneMatch,
      ifUnmodifiedSince,
      requestPayer,
      sseCustomerAlgorithm,
      sseCustomerKey,
      sseCustomerKeyMD5,
      versionId)
  }

  val genGetObjectParams = for {
    bucket           <- Gen.alphaLowerStr
    key              <- Gen.alphaLowerStr
    nBytes           <- genOptionStr
    downloadSettings <- genDownloadSettings

  } yield (bucket, key, nBytes, downloadSettings)

  val genListObjectsParams = for {
    bucket       <- Gen.alphaLowerStr
    marker       <- genOptionStr
    maxKeys      <- Gen.option(Gen.choose(1, 100))
    prefix       <- genOptionStr
    requestPayer <- genOptionStr
  } yield (bucket, marker, maxKeys, prefix, requestPayer)

  val genListObjectsV2Params = for {
    bucket            <- Gen.alphaLowerStr
    continuationToken <- genOptionStr
    fetchOwner        <- genOptionBool
    maxKeys           <- Gen.option(Gen.choose(1, 100))
    prefix            <- genOptionStr
    startAfter        <- genOptionStr
    requestPayer      <- genOptionStr
  } yield (bucket, continuationToken, fetchOwner, maxKeys, prefix, startAfter, requestPayer)

  val genUploadPartParams = for {
    bucket         <- Gen.alphaLowerStr
    key            <- Gen.alphaLowerStr
    partN          <- Gen.choose[Int](1, 1000)
    uploadId       <- Gen.alphaLowerStr
    contentLenght  <- Gen.choose[Long](1, 1000)
    uploadSettings <- genUploadSettings
  } yield (bucket, key, partN, uploadId, contentLenght, uploadSettings)

  val genPutObjectParams = for {
    bucket         <- Gen.alphaLowerStr
    key            <- Gen.alphaLowerStr
    contentLenght  <- Gen.option(Gen.choose[Long](1, 1000))
    uploadSettings <- genUploadSettings
  } yield (bucket, key, contentLenght, uploadSettings)

}
