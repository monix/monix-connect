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
  val randomQueueUrl = "http://localhost:4576/queue/" + randomQueueName
  s"${Sqs}.transformer() creates a transformer operator" that {

    s"given an implicit instance of ${randomQueueName} and ${randomMessageBody} in the scope" must {

      s"transform `SendMessageRequest` to `SendMessageResponse`" in {
        // given

        for {
          _ <- Task.from(client.createQueue(createQueueRequest(randomQueueName)))
          sendTransformer: Transformer[SendMessageRequest, Task[SendMessageResponse]] = Sqs
            .transformer[SendMessageRequest, SendMessageResponse]
          request = sendMessageRequest(queueUrl = randomQueueUrl, messageBody = randomMessageBody)
          response <- Observable
            .pure(request)
            .transform(sendTransformer)
            .headL
            .runToFuture
            .futureValue
          res <- Sqs.source(randomQueueUrl).headL
        } yield {
          response shouldBe a[SendMessageResponse]
          response.md5OfMessageBody() shouldNot be(null)
          res.body() shouldBe randomMessageBody
          Task.from(client.deleteQueue(deleteQueueRequest("http://localhost:4576/queue/" + randomQueueName)))
        }
      }
    }

  }
}
