package monix.connect.sqs

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import SqsOp._

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._

class SqsTransformerSpec
  extends AnyWordSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(20.seconds, 500.milliseconds)
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

      s"transform `DeleteQueueRequest` to `DeleteQueueResponse`" in {
        // given
        val transformer: Transformer[DeleteQueueRequest, Task[DeleteQueueResponse]] =
          Sqs.transformer[DeleteQueueRequest, DeleteQueueResponse]
        val request =
          deleteQueueRequest("http://localhost:4576/queue/" + randomQueueName)

        //when
        val ob: Observable[Task[DeleteQueueResponse]] =
          Observable
            .pure(request)
            .transform(transformer)
        val t: Task[DeleteQueueResponse] = ob.headL.runToFuture.futureValue

        //then
        whenReady(t.runToFuture) { response => response shouldBe a[DeleteQueueResponse] }
      }

    }

  }
}
