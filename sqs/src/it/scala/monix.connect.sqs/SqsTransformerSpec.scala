package monix.connect.sqs

import monix.connect.sqs.SqsOp._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.concurrent.duration._

class SqsTransformerSpec
  extends AnyWordSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val client: SqsAsyncClient = SqsClient()
  val randomQueueName: String = genQueueName.sample.get
  val randomMessageBody: String = genMessageBody.sample.get

  s"${Sqs}.transformer() creates a transformer operator" that {

    s"given an implicit instance of ${randomQueueName} and ${randomMessageBody} in the scope" must {

      s"transform `SendMessageRequest` to `SendMessageResponse`" in {
        // given
        val randomQueueUrl = "http://localhost:4576/queue/" + randomQueueName

        val sendTransformer: Transformer[SendMessageRequest, Task[SendMessageResponse]] =
          Sqs.transformer[SendMessageRequest, SendMessageResponse]
        val request =
          sendMessageRequest(queueUrl = randomQueueUrl, messageBody = randomMessageBody)

        //when
        val ob: Observable[Task[SendMessageResponse]] =
          Observable
            .pure(request)
            .transform(sendTransformer)
        val t: Task[SendMessageResponse] = ob.headL.runToFuture.futureValue

        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[SendMessageResponse]
          response.md5OfMessageBody() shouldNot be(null)
        }

        whenReady(Sqs.source(randomQueueUrl).headL.runToFuture) { res => res.body() shouldBe randomMessageBody }
      }
    }

  }

  override def beforeAll(): Unit = {
    Task.from(client.createQueue(createQueueRequest(randomQueueName)))
    Thread.sleep(3000)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    Task.from(client.deleteQueue(deleteQueueRequest("http://localhost:4576/queue/" + randomQueueName)))
    super.afterAll()
  }
}
