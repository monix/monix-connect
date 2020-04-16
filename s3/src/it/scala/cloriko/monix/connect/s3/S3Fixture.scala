package cloriko.monix.connect.s3

import java.net.URI

import com.amazonaws.auth.{ AWSStaticCredentialsProvider, AnonymousAWSCredentials, BasicAWSCredentials }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions.US_EAST_1
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder }
import S3AppConfig.S3Config
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer
import software.amazon.awssdk.regions.Region.AWS_GLOBAL
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{ GetObjectRequest, GetObjectResponse, PutObjectRequest, PutObjectResponse }
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
trait S3Fixture {
  val s3Config: S3Config = S3AppConfig.load()
  val s3SyncClient = AmazonS3ClientBuilder
    .standard()
    .withPathStyleAccessEnabled(true)
    .withEndpointConfiguration(new EndpointConfiguration(s3Config.endPoint, US_EAST_1.getName))
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("TESTKEY", "TESTSECRET")))
    .build

  val basicAWSCredentials = AwsBasicCredentials.create("TESTKEY", "TESTSECRET")
  val s3AsyncClient = S3AsyncClient
    .builder()
    .credentialsProvider(StaticCredentialsProvider.create(basicAWSCredentials))
    .region(AWS_GLOBAL)
    .endpointOverride(URI.create(s3Config.endPoint))
    .build

  def getRequest(bucket: String, key: String): GetObjectRequest =
    GetObjectRequest.builder().bucket(bucket).key(key).build()

  def asyncStringTransformer: ByteArrayAsyncResponseTransformer[String] = new ByteArrayAsyncResponseTransformer[String]
}
