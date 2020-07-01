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
  var queueToDelete = ""

  s"${Sqs}.transformer() creates a transformer operator" that {

    s"given an implicit instance of ${client} in the scope" must {
      s"transform `CreateQeueuequests` to `CreateQueueResponses`" in {
        // given
        val randomQueueName: String = genQueueName.sample.get
        queueToDelete = "http://localhost:4576/queue/" + randomQueueName
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
      /**s"transform `SendMessageRequest` to `SendMessageResponse`" in {
        // given
        val listTransformer: Transformer[ListQueuesRequest, Task[ListQueuesResponse]] =
          Sqs.transformer[ListQueuesRequest, ListQueuesResponse]

        val queueName = Observable
          .pure(listQueuesRequest(""))
          .transform(listTransformer)
          .headL
          .runSyncUnsafe()

        val randomQueueUrl: String = queueName.runSyncUnsafe().queueUrls().get(0)
        queueToDelete = randomQueueUrl
        val randomMessageBody: String = genMessageBody.sample.get
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
        whenReady(t.runToFuture) { response => response shouldBe a[SendMessageResponse] }

        //when
        val receiveTransformer: Transformer[ReceiveMessageRequest, Task[ReceiveMessageResponse]] =
          Sqs.transformer[ReceiveMessageRequest, ReceiveMessageResponse]
        val request2 =
          receiveMessageRequest(queueUrl = randomQueueUrl)
        val ob2: Observable[Task[ReceiveMessageResponse]] =
          Observable
            .pure(request2)
            .transform(receiveTransformer)
        val tt: Task[ReceiveMessageResponse] = ob2.headL.runToFuture.futureValue
        //then
        whenReady(tt.runToFuture) { response =>
          response shouldBe a[ReceiveMessageResponse]
          response.messages().get(0).body() shouldBe randomMessageBody
        }
      }*/
    }

  }

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    val transformer: Transformer[DeleteQueueRequest, Task[DeleteQueueResponse]] =
      Sqs.transformer[DeleteQueueRequest, DeleteQueueResponse]
    val t = Observable
      .pure(deleteQueueRequest(queueToDelete))
      .transform(transformer)
      .headL
      .runToFuture
      .futureValue
      .delayExecution(2.seconds)
    whenReady(t.runToFuture) { res => super.afterAll() }

  }
}
