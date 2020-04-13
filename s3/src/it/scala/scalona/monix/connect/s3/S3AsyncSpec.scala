package scalona.monix.connect.s3

import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture

import com.amazonaws.services.s3.model.{ Grant, Owner }
import monix.eval.Task
import org.scalacheck.Gen
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }
import software.amazon.awssdk.services.s3.model.{ CreateBucketRequest, GetObjectAclResponse, PutObjectRequest, PutObjectResponse }

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.jdk.FutureConverters._
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer

import scala.util.Try
class S3AsyncSpec
  extends WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with S3Fixture with Eventually {

  private val bucketName = "sample-bucket"
  implicit val s3Client = s3AsyncClient

  override implicit val patienceConfig = PatienceConfig(10.seconds, 100.milliseconds)

  s"${S3}" should {
    "implement putObject method" that {

      s"uploads the passed ByteBuffer to the respective s3 bucket and key" when {

        "contentLength and contentType are not defined and therefore infered by the method" in {
          //given
          val key = Gen.alphaLowerStr.sample.get
          val content = Gen.alphaUpperStr.sample.get

          //when
          val t: Task[PutObjectResponse] = S3.putObject(bucketName, key, ByteBuffer.wrap(content.getBytes()))

          whenReady(t.runToFuture) { putResponse =>
            val s3Object: ByteBuffer = S3.getObject(bucketName, key).runSyncUnsafe()
            putResponse shouldBe a[PutObjectResponse]
            s3Object.array() shouldBe content.getBytes()
          }
        }

        "contentLength and contentType are defined respectively as the array lenght and 'application/json'" in {
          //given
          val key = Gen.alphaLowerStr.sample.get
          val content = Gen.alphaUpperStr.sample.get

          //when
          val t: Task[PutObjectResponse] = S3.putObject(
            bucketName,
            key,
            ByteBuffer.wrap(content.getBytes()),
            Some(content.length),
            Some("appliction/json"))

          whenReady(t.runToFuture) { putResponse =>
            val s3Object: ByteBuffer = S3.getObject(bucketName, key).runSyncUnsafe()
            putResponse shouldBe a[PutObjectResponse]
            s3Object.array() shouldBe content.getBytes()
          }
        }
      }

      s"downloads a ByteBuffer of an existing s3 object" in {
        //given
        val key = Gen.alphaLowerStr.sample.get
        val content = Gen.alphaUpperStr.sample.get
        s3SyncClient.putObject(bucketName, key, content)

        //when
        val t: Task[ByteBuffer] = S3.getObject(bucketName, key)

        whenReady(t.runToFuture) { actualContent: ByteBuffer =>
          s3SyncClient.doesObjectExist(bucketName, key) shouldBe true
          actualContent shouldBe a[ByteBuffer]
          actualContent.array() shouldBe content.getBytes()
        }
      }

    }
    /*"correctly upload a single S3Object to S3" in {
      //given
      val key: String = Gen.alphaLowerStr.sample.get
      val content: String = Gen.alphaLowerStr.sample.get
      val s3Sink: Consumer[S3Object, Either[Throwable, PutObjectResult]] = S3.putObject

      //when
      val maybePutResult: Either[Throwable, PutObjectResult] = {
        Observable
          .fromIterable(List(S3Object(bucketName, key, content)))
          .consumeWith(s3Sink)
          .runSyncUnsafe()
      }

      //then
      maybePutResult.isRight shouldBe true
      maybePutResult.right.get shouldBe a[PutObjectResult]
      //val actualContent: String = S3Client().getObjectAsString(bucketName, key) todo check s3 file content
      //content shouldBe actualContent
    }*/
  }

  /*"correctly upload a collection of S3Objects in S3" in {
    //given
    val key: String = Gen.alphaLowerStr.sample.get
    val content: String = Gen.alphaLowerStr.sample.get
    val s3Sink: Consumer[S3Object, Either[Throwable, PutObjectResult]] = S3.putObject

    //when
    val maybePutResult: Either[Throwable, PutObjectResult] = {
      Observable
        .fromIterable(List(S3Object(bucketName, key, content)))
        .consumeWith(s3Sink)
        .runSyncUnsafe()
    }

    //then
    maybePutResult.isRight shouldBe true
    maybePutResult.right.get shouldBe a[PutObjectResult]
    //val actualContent: String = S3Client().getObjectAsString(bucketName, key) todo check s3 file content
    //content shouldBe actualContent
  }*/

  override def beforeAll(): Unit = {
    super.beforeAll()
    //val createBucketRequest: CreateBucketRequest = CreateBucketRequest.builder().bucket(bucketName).build()
    //Await.result(s3Client.createBucket(createBucketRequest).asScala, 5.seconds)
    Try(s3SyncClient.createBucket(bucketName))
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
