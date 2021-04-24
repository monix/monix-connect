package monix.connect.sqs

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class SqsSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val randomQueueName: String = genQueueName.sample.get

  //todo create a queue with attributes
  s"Sqs" can "create queue and check it exists" in {
    val queueName = genQueueName.sample.get + "wdaw"

    Sqs.fromConfig.use { sqs =>
      for {
        existedBefore <- sqs.existsQueue(queueName)
        _ <- sqs.createQueue(queueName)
        existsAfter <- sqs.existsQueue(queueName)
      } yield {
        existedBefore shouldBe false
        existsAfter shouldBe true
      }
    }.runSyncUnsafe()
  }

  it can "get a queue and queue url from its name" in {
    val queueName = genQueueName.sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        _ <- sqs.createQueue(queueName)
        queueUrl <- sqs.getQueueUrl(queueName)
        emptyQueueUrl <- sqs.getQueueUrl("randomName123")
      } yield {
        queueUrl shouldBe Some(queueUrlPrefix(queueName))
        emptyQueueUrl shouldBe None
      }
    }.runSyncUnsafe()
  }

  it can "delete by name" in {
    val queue1 = genQueueName.sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        existsBeforeDelete <- sqs.createQueue(queue1) >> sqs.existsQueue(queue1)
        existsAfterDelete <- sqs.deleteQueueByName(queue1) >> sqs.existsQueue(queue1)
      } yield {
        existsBeforeDelete shouldBe true
        existsAfterDelete shouldBe false
      }
    }.runSyncUnsafe()
  }

  it can "delete by url" in {
    val queue = genQueueName.sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        existsBeforeDelete <- sqs.createQueue(queue) >> sqs.existsQueue(queue)
        queueName <- sqs.getQueueUrl(queue)
        _ <- queueName.map(sqs.deleteQueueByUrl).getOrElse(Task.unit)
        existsAfterDelete <- sqs.existsQueue(queue)
      } yield {
        existsBeforeDelete shouldBe true
        existsAfterDelete shouldBe false
      }
    }.runSyncUnsafe()
  }

  override def beforeAll(): Unit = {
   // Task.from(asyncClient.createQueue(createQueueRequest(randomQueueName))).runSyncUnsafe()
    Thread.sleep(3000)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    //Task.from(client.deleteQueue(deleteQueueRequest("http://localhost:4576/queue/" + randomQueueName)))
    super.afterAll()
  }
}
