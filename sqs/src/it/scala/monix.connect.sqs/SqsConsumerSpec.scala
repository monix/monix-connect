package monix.connect.sqs

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import scala.concurrent.duration._

class SqsConsumerSpec extends AnyWordSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val client: SqsAsyncClient = SqsClient()
  val randomQueueName: String = genQueueName.sample.get

  s"${Sqs}.consumer() creates a Monix ${Consumer}" that {

    s"given an implicit queue name of ${randomQueueName} in the scope" must {

      s"consume a single `CreateQueueRequest` and materializes to `CreateQueueResponse`" in {
        // given
        val consumer: Consumer[CreateQueueRequest, CreateQueueResponse] =
          Sqs.consumer[CreateQueueRequest, CreateQueueResponse]
        val request =
          createQueueRequest(queueName = randomQueueName)

        //when
        val t: Task[CreateQueueResponse] = Observable.pure(request).consumeWith(consumer)

        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[CreateQueueResponse]
          response.queueUrl() shouldBe "http://localhost:4576/queue/" + randomQueueName
        }
      }

      s"consume a single `ListQueuesRequest` and materializes to `ListQueuesResponse`" in {
        // given
        val consumer: Consumer[ListQueuesRequest, ListQueuesResponse] =
          Sqs.consumer[ListQueuesRequest, ListQueuesResponse]
        val request =
          listQueuesRequest("")

        //when
        val t: Task[ListQueuesResponse] = Observable.pure(request).consumeWith(consumer)

        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[ListQueuesResponse]
          response.queueUrls().size() shouldBe 1
          response.queueUrls().get(0) shouldBe "http://localhost:4576/queue/" + randomQueueName
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
