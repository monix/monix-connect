package monix.connect.s3

import java.time.Instant

import org.scalacheck.Gen
import software.amazon.awssdk.services.s3.model.CompletedPart

trait S3RequestGenerators {

  val genOptionStr = Gen.option(Gen.alphaLowerStr)
  val genOptionBool = Gen.option(Gen.oneOf(true, false))

  val genCreateBucketParams = for {
    bucket           <- Gen.alphaLowerStr
    acl              <- genOptionStr
    grantFullControl <- genOptionStr
    grantRead        <- genOptionStr
    grantReadACP     <- genOptionStr
    grantWrite       <- genOptionStr
    grantWriteACP    <- genOptionStr
  } yield (bucket, acl, grantFullControl, grantRead, grantReadACP, grantWrite, grantWriteACP)

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
    requestPayer   <- genOptionStr
  } yield (bucket, key, uploadId, completedParts, requestPayer)

  val genCreateMultipartUploadParams = for {
    bucket                  <- Gen.alphaLowerStr
    key                     <- Gen.alphaLowerStr
    contentType             <- genOptionStr
    acl                     <- genOptionStr
    grantFullControl        <- genOptionStr
    grantRead               <- genOptionStr
    grantReadACP            <- genOptionStr
    grantWriteACP           <- genOptionStr
    requestPayer            <- genOptionStr
    serverSideEncryption    <- genOptionStr
    sseCustomerAlgorithm    <- genOptionStr
    sseCustomerKey          <- genOptionStr
    sseCustomerKeyMD5       <- genOptionStr
    ssekmsEncryptionContext <- genOptionStr
    ssekmsKeyId             <- genOptionStr
  } yield (
    bucket,
    key,
    contentType,
    acl,
    grantFullControl,
    grantRead,
    grantReadACP,
    grantWriteACP,
    requestPayer,
    serverSideEncryption,
    sseCustomerAlgorithm,
    sseCustomerKey,
    sseCustomerKeyMD5,
    ssekmsEncryptionContext,
    ssekmsKeyId)

  val genDeleteObjectParams = for {
    bucket                    <- Gen.alphaLowerStr
    key                       <- Gen.alphaLowerStr
    bypassGovernanceRetention <- genOptionBool
    mfa                       <- genOptionStr
    requestPayer              <- genOptionStr
    versionId                 <- genOptionStr
  } yield (bucket, key, bypassGovernanceRetention, mfa, requestPayer, versionId)

  val genGetObjectParams = for {
    bucket               <- Gen.alphaLowerStr
    key                  <- Gen.alphaLowerStr
    ifMatch              <- genOptionStr
    ifModifiedSince      <- Gen.option(Gen.oneOf(Seq(Instant.now())))
    ifNoneMatch          <- genOptionStr
    ifUnmodifiedSince    <- Gen.option(Gen.oneOf(Seq(Instant.now())))
    partNumber           <- Gen.option(Gen.chooseNum[Int](1, 200))
    range                <- genOptionStr
    requestPayer         <- genOptionStr
    sseCustomerAlgorithm <- genOptionStr
    sseCustomerKey       <- genOptionStr
    sseCustomerKeyMD5    <- genOptionStr
    versionId            <- genOptionStr
  } yield (
    bucket,
    key,
    ifMatch,
    ifModifiedSince,
    ifNoneMatch,
    ifUnmodifiedSince,
    partNumber,
    range,
    requestPayer,
    sseCustomerAlgorithm,
    sseCustomerKey,
    sseCustomerKeyMD5,
    versionId
  )

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
    bucket               <- Gen.alphaLowerStr
    key                  <- Gen.alphaLowerStr
    partN                <- Gen.choose[Int](1, 1000)
    uploadId             <- Gen.alphaLowerStr
    contentLenght        <- Gen.choose[Long](1, 1000)
    requestPayer         <- genOptionStr
    sseCustomerAlgorithm <- genOptionStr
    sseCustomerKey       <- genOptionStr
    sseCustomerKeyMD5    <- genOptionStr
  } yield (
    bucket,
    key,
    partN,
    uploadId,
    contentLenght,
    requestPayer,
    sseCustomerAlgorithm,
    sseCustomerKey,
    sseCustomerKeyMD5)

  val genPutObjectParams = for {
    bucket                  <- Gen.alphaLowerStr
    key                     <- Gen.alphaLowerStr
    contentLenght           <- Gen.option(Gen.choose[Long](1, 1000))
    contentType             <- genOptionStr
    acl                     <- genOptionStr
    grantFullControl        <- genOptionStr
    grantRead               <- genOptionStr
    grantReadACP            <- genOptionStr
    grantWriteACP           <- genOptionStr
    requestPayer            <- genOptionStr
    serverSideEncryption    <- genOptionStr
    sseCustomerAlgorithm    <- genOptionStr
    sseCustomerKey          <- genOptionStr
    sseCustomerKeyMD5       <- genOptionStr
    ssekmsEncryptionContext <- genOptionStr
    ssekmsKeyId             <- genOptionStr
  } yield (
    bucket,
    key,
    contentLenght,
    contentType,
    acl,
    grantFullControl,
    grantRead,
    grantReadACP,
    grantWriteACP,
    requestPayer,
    serverSideEncryption,
    sseCustomerAlgorithm,
    sseCustomerKey,
    sseCustomerKeyMD5,
    ssekmsEncryptionContext,
    ssekmsKeyId)

}
