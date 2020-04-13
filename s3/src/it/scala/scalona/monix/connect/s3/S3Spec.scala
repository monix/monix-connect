package scalona.monix.connect.s3

import monix.reactive.{Consumer, Observable}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import scalona.monix.connect.s3.domain.S3Object

class S3Spec extends WordSpecLike with Matchers with BeforeAndAfterAll with S3Fixture {

  private val bucketName = "sample-bucket"
  implicit val s3Client = AsyncClient()

  s"The ${S3}.sink" should {
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

  "download the correspondent s3 object as string" in {
    //given
    val key = Gen.alphaLowerStr.sample.get
    val content = Gen.alphaUpperStr.sample.get
    //AsyncClient().createBucket(bucketName, key, content)

    //when
    //val downloadContent = AsyncClient().getObjectAsString(bucketName, key)

    //then
    //content shouldEqual downloadContent
}

  override def beforeAll(): Unit = {
    super.beforeAll()
    //AsyncClient().createBucket(bucketName)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
