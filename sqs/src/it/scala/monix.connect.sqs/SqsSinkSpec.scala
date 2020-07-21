package monix.connect.sqs

import monix.eval.Task
import monix.reactive.{Consumer, Observable, OverflowStrategy}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._
import monix.connect.sqs.SqsOp.Implicits._
import monix.execution.Scheduler.Implicits.global
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._
import scala.util.Failure



class SqsSinkSpec extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterEach {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(2.seconds, 300.milliseconds)
  implicit val client: SqsAsyncClient = SqsClient()

  val queueName: String = genQueueName.sample.get
  val queueUrl = getQueueUrl(queueName)

  s"${Sqs}.sink()" should "sink a single `SendMessageRequest` and materialize to `Unit`" in {
    //given
    val body: String = genMessageBody.sample.get
    val request = sendMessageRequest(queueUrl = queueUrl, messageBody = body)
    val sink: Consumer[SendMessageRequest, Unit] = Sqs.sink

    //when
    val t: Task[Unit] = Observable.pure(request).consumeWith(sink)

    //then
    whenReady(t.runToFuture) { r =>
      r shouldBe a[Unit]
      val message = Sqs.source(queueUrl).firstL.runSyncUnsafe()
      body shouldBe message.body()
    }
  }

  it should "be resilient when the Observable is empty" in {
    //given
    val sink: Consumer[SendMessageRequest, Unit] = Sqs.sink

    //when
    val t: Task[Unit] = Observable.empty.consumeWith(sink)

    //then
    val maybeMessage = Sqs.source(queueUrl).firstOptionL.runToFuture
    Thread.sleep(1000)
    whenReady(t.runToFuture) { r =>
      r shouldBe a[Unit]
      maybeMessage.value shouldBe None
    }
  }

  override def beforeEach() = {
    super.beforeEach()
    SqsOp.create(createQueueRequest(queueName)).runSyncUnsafe()
  }

  override def afterEach() = {
    super.afterEach()
    SqsOp.create(deleteQueueRequest(queueUrl)).runSyncUnsafe()
  }

}
