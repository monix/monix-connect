package monix.connect.s3

import java.io.{File, FileInputStream}
import java.net.URI
import java.time.Duration
import monix.eval.{Task, TaskLike}
import monix.execution.Scheduler
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.AsyncTestSuite
import software.amazon.awssdk.regions.Region.AWS_GLOBAL
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region

trait S3Fixture {
  this: AsyncTestSuite =>

  val genBucketName = Gen.identifier.map("test-"+ _.take(15).toLowerCase) //buckets have to be in a range of 3-63 chars long
  val genKey = Gen.identifier.map("test-"+ _.take(10).toLowerCase) //buckets have to be in a range of 3-63 chars long
  val resourceFile = (fileName: String) => s"s3/src/it/resources/${fileName}"

  val minioEndPoint: String = "http://localhost:9000"

  val s3AccessKey: String = "TESTKEY"
  val s3SecretKey: String = "TESTSECRET"

  val httpClient = NettyNioAsyncHttpClient.builder()
    .maxConcurrency(500)
    .maxPendingConnectionAcquires(50000)
    .connectionAcquisitionTimeout(Duration.ofSeconds(60))
    .readTimeout(Duration.ofSeconds(60))
    .build();

  val basicAWSCredentials = AwsBasicCredentials.create(s3AccessKey, s3SecretKey)
  val staticCredProvider = StaticCredentialsProvider.create(basicAWSCredentials)
  implicit val s3AsyncClient: S3AsyncClient = S3AsyncClient
    .builder()
    .httpClient(httpClient)
    .credentialsProvider(staticCredProvider)
    .region(AWS_GLOBAL)
    .endpointOverride(URI.create(minioEndPoint))
    .build

  protected val s3Resource = S3.create(staticCredProvider, Region.AWS_GLOBAL, Some(minioEndPoint), Some(httpClient))
  protected val unsafeS3 = S3.createUnsafe(staticCredProvider, Region.AWS_GLOBAL, Some(minioEndPoint), Some(httpClient))

  def getRequest(bucket: String, key: String): GetObjectRequest =
    GetObjectRequest.builder().bucket(bucket).key(key).build()

  def unsafeDownload(bucket: String, key: String)(implicit scheduler: Scheduler): Option[Array[Byte]] = {
    val s3LocalPath = s"minio/data/${bucket}/${key}"
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

  implicit val fromGen: TaskLike[Gen] =
    new TaskLike[Gen] {
      def apply[A](fa: Gen[A]): Task[A] = Task(fa.sample.get)
    }
}
