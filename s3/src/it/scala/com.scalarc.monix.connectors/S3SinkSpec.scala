package com.scalarc.monix.connectors.s3

import com.amazonaws.services.s3.model.{ObjectListing, PutObjectRequest, PutObjectResult}
import com.scalarc.monix.connectors.s3.S3Sink.S3Object
import monix.eval.Task
import monix.reactive.Observable
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
class S3SinkSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  private val bucketName = "sample-bucket"

  "Monix S3Sink" should {
    "correctly upload a string payload to an object in S3" in {
      val key: String = Gen.alphaLowerStr.sample.get
      val content: String = Gen.alphaLowerStr.sample.get
      Observable
        .fromIterable(List(S3Object(bucketName, key, content)))
        .consumeWith(new S3Sink().putAndForget())
        .runSyncUnsafe()
      println(s"Bucket: $bucketName, Key: $key")
      val actualContent: String = S3Client().getObjectAsString(bucketName, key)
      content shouldBe actualContent
    }
  }

  "get object as string" should  {
    "return the object content " in {
      val actualContent: String = S3Client().getObjectAsString("sample-bucket", "wfuoswjrolhdofgjgacbfyvcydbnarelyuucqlepdurrof")


    }
  }
  override def beforeAll(): Unit = {
    super.beforeAll()
    S3Client().createBucket(bucketName)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    //S3Client().deleteBucket(bucketName)
  }
}
