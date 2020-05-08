package monix.connect.s3

import java.io.{File, FileInputStream}
import java.net.URI
import java.time.Instant

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions.US_EAST_1
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.TestSuite
import software.amazon.awssdk.regions.Region.AWS_GLOBAL
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CompletedPart, GetObjectRequest}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

trait S3Fixture {
  this: TestSuite =>

  val resourceFile = (fileName: String) => s"s3/src/it/resources/${fileName}"

  val minioEndPoint: String = "http://localhost:9000"

  val s3AccessKey: String = "TESTKEY"
  val s3SecretKey: String = "TESTSECRET"

  val s3SyncClient = AmazonS3ClientBuilder
    .standard()
    .withPathStyleAccessEnabled(true)
    .withEndpointConfiguration(new EndpointConfiguration(minioEndPoint, US_EAST_1.getName))
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3AccessKey, s3SecretKey)))
    .build

  val basicAWSCredentials = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
  val s3AsyncClient = S3AsyncClient
    .builder()
    .credentialsProvider(StaticCredentialsProvider.create(basicAWSCredentials))
    .region(AWS_GLOBAL)
    .endpointOverride(URI.create(minioEndPoint))
    .build

  def getRequest(bucket: String, key: String): GetObjectRequest =
    GetObjectRequest.builder().bucket(bucket).key(key).build()

  def download(bucketName: String, key: String)(implicit scheduler: Scheduler): Option[Array[Byte]] = {
    val s3LocalPath = s"minio/data/${bucketName}/${key}"
    downloadFromFile(s3LocalPath)
  }

  def downloadFromFile(filePath: String)(implicit scheduler: Scheduler): Option[Array[Byte]] = {
    val file = new File(filePath)
    if (file.exists()) {
      val inputStream: Task[FileInputStream] = Task(new FileInputStream(file))
      val ob: Observable[Array[Byte]] = Observable.fromInputStream(inputStream)
      val content: Array[Byte] = ob.foldLeft(Array.emptyByteArray)((acc, bytes) => acc ++ bytes).headL.runSyncUnsafe()
      Some(content)
    } else {
      println(s"The file ${file} does not exist, returning None")
      None
    }
  }

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
