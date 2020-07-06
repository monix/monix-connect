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

      s"transform `CreateQueueRequest` to `CreateQueueResponse`" in {
        // given
        val transformer: Transformer[CreateQueueRequest, Task[CreateQueueResponse]] =
          Sqs.transformer[CreateQueueRequest, CreateQueueResponse]
        val request =
          createQueueRequest(queueName = randomQueueName)

        //when
        val ob: Observable[Task[CreateQueueResponse]] =
          Observable
            .pure(request)
            .transform(transformer)
        val t: Task[CreateQueueResponse] = ob.headL.runToFuture.futureValue

        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[CreateQueueResponse]
          response.queueUrl() shouldBe "http://localhost:4576/queue/" + randomQueueName
        }
      }

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
      }

      s"transform `ReceiveMessageRequest` to `ReceiveMessageResponse`" in {
        //given
        val randomQueueUrl = "http://localhost:4576/queue/" + randomQueueName

        //when
        val receiveTransformer: Transformer[ReceiveMessageRequest, Task[ReceiveMessageResponse]] =
          Sqs.transformer[ReceiveMessageRequest, ReceiveMessageResponse]
        val request =
          receiveMessageRequest(queueUrl = randomQueueUrl)
        val ob2: Observable[Task[ReceiveMessageResponse]] =
          Observable
            .pure(request)
            .transform(receiveTransformer)
        val t: Task[ReceiveMessageResponse] = ob2.headL.runToFuture.futureValue
        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[ReceiveMessageResponse]
          response.messages().get(0).body() shouldBe randomMessageBody
        }
      }

    }

  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    Task.from(client.deleteQueue(deleteQueueRequest("http://localhost:4576/queue/" + randomQueueName)))
    super.afterAll()
  }
}
