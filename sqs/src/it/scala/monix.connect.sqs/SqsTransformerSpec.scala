package monix.connect.sqs

import monix.eval.Task
import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import monix.connect.sqs.SqsOp.Implicits._
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

class SqsTransformerSpec
  extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val client: SqsAsyncClient = SqsClient()

  val queuePrefix = "transformer-"
  val randomQueueName: String = queuePrefix + genQueueName.sample.get
  val randomQueueUrl = queueUrl(randomQueueName)

  override def beforeAll() = {
    super.beforeAll()
    SqsOp.create(createQueueRequest(randomQueueName)).runSyncUnsafe()
  }

  override def afterAll() = {
    super.afterAll()
    SqsOp.create(deleteQueueRequest(randomQueueUrl)).runSyncUnsafe()
  }

  s"${Sqs}.transformer() creates a transformer operator" should "transform `SendMessageRequest` to `SendMessageResponse`" in {
     // given
     val randomMessageBody: String = genMessageBody.sample.get
     val sendTransformer = Sqs.transformer[SendMessageRequest, SendMessageResponse]
     val request =  sendMessageRequest(queueUrl = randomQueueUrl, messageBody = randomMessageBody)

     //when
     val t: Task[SendMessageResponse] = Observable.pure(request).transform(sendTransformer).headL.flatten

     whenReady(t.runToFuture) { response =>
       response shouldBe a[SendMessageResponse]
       response.md5OfMessageBody() shouldNot be(null)
     }
  }

  it should "transform `ListQueuesRequest` to `ListQueuesResponse`" in {
    // given
    val sendTransformer = Sqs.transformer[ListQueuesRequest, ListQueuesResponse]
    val request = listQueuesRequest("transformer-test")

    //when
    val t: Task[ListQueuesResponse] = Observable.pure(request).transform(sendTransformer).headL.flatten

    whenReady(t.runToFuture) { r =>
      r shouldBe a[ListQueuesResponse]
      r.queueUrls().size() shouldBe 1
      r.queueUrls().get(0) shouldBe randomQueueUrl
    }
  }
}
