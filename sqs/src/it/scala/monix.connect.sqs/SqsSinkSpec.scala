package monix.connect.sqs

import monix.eval.Task
import monix.reactive.{Consumer, Observable, OverflowStrategy}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import monix.connect.sqs.SqsOp.Implicits._
import monix.connect.sqs.SqsOp
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

class SqsSinkSpec extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val client: SqsAsyncClient = SqsClient()

  val randomQueueName: String = genQueueName.sample.get
  val randomQueueUrl = queueUrl(randomQueueName)

  override def beforeAll() = {
    super.beforeAll()
    SqsOp.create(createQueueRequest(randomQueueName)).runSyncUnsafe()
  }

  override def afterAll() = {
    super.afterAll()
    SqsOp.create(deleteQueueRequest(randomQueueUrl)).runSyncUnsafe()
  }

  s"${Sqs}.consumer()" should "consume a single `ListQueuesRequest` and materialize to `ListQueuesResponse`" in {
    //given
    val randomMessageBody: String = genMessageBody.sample.get
    val request = sendMessageRequest(queueUrl = randomQueueUrl, messageBody = randomMessageBody)
    val sink: Consumer[SendMessageRequest, Unit] = Sqs.sink

    //when
    val t: Task[Unit] = Observable.pure(request).consumeWith(sink)

    //then
    whenReady(t.runToFuture) { res =>
      res shouldBe a[Unit]
      //todo consume queue to check that the sqs message has correctly been published
    }
  }

}
