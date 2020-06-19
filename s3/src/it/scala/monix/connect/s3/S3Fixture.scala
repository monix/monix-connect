package monix.connect.s3

import java.io.{File, FileInputStream}
import java.net.URI

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions.US_EAST_1
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalatest.TestSuite
import software.amazon.awssdk.regions.Region.AWS_GLOBAL
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
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
  val s3AsyncClient: S3AsyncClient = S3AsyncClient
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

}
