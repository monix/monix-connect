package monix.connect.sqs

import monix.eval.Task
import monix.reactive.{Consumer, Observable}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import monix.connect.sqs.SqsOp.Implicits._
import monix.connect.sqs.SqsOp

import scala.concurrent.duration._

class SqsConsumerSpec extends AnyWordSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val client: SqsAsyncClient = SqsClient()
  val randomQueueName: String = genQueueName.sample.get

  s"${Sqs}.consumer() creates a Monix ${Consumer}" that {

    s"given an implicit queue name of ${randomQueueName} in the scope" must {

      s"consume a single `ListQueuesRequest` and materializes to `ListQueuesResponse`" in {
        // given

        for {
          _ <- SqsOp.create(createQueueRequest(randomQueueName))
          consumer = Sqs.sink[ListQueuesRequest, ListQueuesResponse]
          request = listQueuesRequest("")
          res <- Observable.pure(request).consumeWith(consumer)
        } yield {
          res shouldBe a[ListQueuesResponse]
          res.queueUrls().size() shouldBe 1
          res.queueUrls().get(0) shouldBe "http://localhost:4576/queue/" + randomQueueName
          SqsOp.create(deleteQueueRequest("http://localhost:4576/queue/" + randomQueueName))
        }
      }
    }
  }
}
