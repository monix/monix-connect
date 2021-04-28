package monix.connect.sqs

import monix.connect.sqs.domain.{InboundMessage, QueueName, QueueUrl}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.commons.codec.digest.DigestUtils.{md2Hex, md5Hex}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.{QueueAttributeName, QueueDoesNotExistException}

import scala.concurrent.duration._

class SqsSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get

  //todo create a queue with attributes
  s"Sqs" can "create queue and check it exists" in {

    Sqs.fromConfig.use { sqs =>
      for {
        existedBefore <- sqs.operator.existsQueue(queueName)
        _ <- sqs.operator.createQueue(queueName)
        existsAfter <- sqs.operator.existsQueue(queueName)
      } yield {
        existedBefore shouldBe false
        existsAfter shouldBe true
      }
    }.runSyncUnsafe()
  }

  it can "create tags, add new ones and list them" in {
    val queueName = genQueueName.sample.get
    Sqs.fromConfig.use { sqs =>
      val initialTags = Map("mode" -> "difficult")
      val tagsByUrl = Map("environment" -> "test")

      for {
        queueUrl <- sqs.operator.createQueue(queueName, tags = initialTags)
        _ <- sqs.operator.tagQueue(queueUrl, tags = tagsByUrl) //tagging by url
        errorMessage <- sqs.operator.tagQueue(QueueUrl("nonExistingQueue"), tags = tagsByUrl)
          .as(None).onErrorHandle(ex => Some(ex.getMessage))
        appliedTags <- sqs.operator.listQueueTags(queueUrl)
      } yield {
        appliedTags shouldBe initialTags ++ tagsByUrl
        errorMessage.isDefined shouldBe true
        errorMessage shouldBe Some(nonExistingQueueErrorMsg)
      }
    }.runSyncUnsafe()
  }

  it can "remove tags from a queue" in {
    val queueName = genQueueName.sample.get
    Sqs.fromConfig.use { sqs =>
      val queueType = "type"
      val modeTagKey = "mode"
      val environmentTagKey = "environment"
      val dummyTagKey = "dummy"
      val initialTags = Map(queueType -> "1", modeTagKey -> "difficult",
        environmentTagKey -> "test", dummyTagKey -> "123")
      for {
        queueUrl <- sqs.operator.createQueue(queueName, tags = initialTags)
        untagByUrl <- sqs.operator.untagQueue(queueUrl, tagKeys = List(queueType)) >>
          sqs.operator.listQueueTags(queueUrl)
        untagByName <- sqs.operator.untagQueue(queueUrl, tagKeys = List(modeTagKey, environmentTagKey)) >> //tagging by queue name
          sqs.operator.listQueueTags(queueUrl)
        errorMessage <- sqs.operator.untagQueue(QueueUrl("nonExistingQueue"), tagKeys = List(dummyTagKey))
          .as(None).onErrorHandle(ex => Some(ex.getMessage))
      } yield {
        untagByUrl shouldBe initialTags.filterNot(kv => kv._1 == queueType)
        untagByName shouldBe Map(dummyTagKey -> "123")
        errorMessage.isDefined shouldBe true
        errorMessage shouldBe Some(nonExistingQueueErrorMsg)
      }
    }.runSyncUnsafe()
  }

  it can "create attributes, add new ones and list them" in {
    val queueName = genQueueName.sample.get
    Sqs.fromConfig.use { sqs =>
      val initialAttributes = Map(QueueAttributeName.DELAY_SECONDS -> "60")
      val attributesByUrl =  Map(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS -> "12345")

      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = initialAttributes)
        _ <- sqs.operator.setQueueAttributes(queueUrl, attributes = attributesByUrl)
        errorMessage <- sqs.operator.setQueueAttributes(QueueUrl("randomUrl"), attributes = attributesByUrl)
          .as(None).onErrorHandle(ex => Some(ex.getMessage))
        attributes <- sqs.operator.getQueueAttributes(queueUrl)
      } yield {
        val expectedAttributes = initialAttributes ++ attributesByUrl
        attributes.filter(kv => expectedAttributes.keys.toList.contains(kv._1)) should contain theSameElementsAs expectedAttributes
        errorMessage.isDefined shouldBe true
        errorMessage shouldBe Some(nonExistingQueueErrorMsg)
      }
    }.runSyncUnsafe()
  }

  it can "get a queue and queue url from its name" in {
    val queueName = genQueueName.sample.get
    Sqs.fromConfig.use { case Sqs(receiver, producer, operator) =>
      for {
        createdQueueUrl <- operator.createQueue(queueName)
        queueUrl <- operator.getQueueUrl(queueName)
        emptyQueueUrl <- operator.getQueueUrl(QueueName("randomName123")).onErrorHandleWith { ex =>
          if (ex.isInstanceOf[QueueDoesNotExistException]) {
            Task.now(Option.empty)
          } else {
            Task.raiseError(ex)
          }
        }
      } yield {
        createdQueueUrl shouldBe QueueUrl(queueUrlPrefix(queueName.name))
        queueUrl shouldBe QueueUrl(queueUrlPrefix(queueName.name))
        emptyQueueUrl shouldBe None
      }
    }.runSyncUnsafe()
  }

  it can "delete queue by url" in {
    val queueName = genQueueName.sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName)
        existsBefore <- sqs.operator.existsQueue(queueName)
        existsAfterDelete <- sqs.operator.deleteQueue(queueUrl) >> sqs.operator.existsQueue(queueName)
        errorMessage <- sqs.operator.deleteQueue(queueUrl).as(None).onErrorHandle(ex => Some(ex.getMessage))
      } yield {
        existsBefore shouldBe true
        existsAfterDelete shouldBe false
        errorMessage.isDefined shouldBe true
        errorMessage.get.contains("AWS.SimpleQueueService.NonExistentQueue") shouldBe true
      }
    }.runSyncUnsafe()
  }

  it can "list all queue urls" in {
    val queueNames = Gen.listOfN(10, genQueueName).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrls <- Task.traverse(queueNames)(sqs.operator.createQueue(_))
        fullQueueList <- sqs.operator.listAllQueueUrls.toListL
      } yield {
        fullQueueList should contain theSameElementsAs queueUrls
      }
    }.runSyncUnsafe()
  }

  it can "list queue by the name prefix name" in {
    val prefix = Gen.identifier.map(id => s"test-${id.take(5)}").sample.get
    val n = 6
    val nonPrefixedQueueNames = Gen.listOfN(10, genQueueName).sample.get
    val prefixedQueueNames = Gen.listOfN(n, genQueueName.map(prefix + _.name).map(QueueName)).sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        _ <- Task.traverse(nonPrefixedQueueNames)(sqs.operator.createQueue(_))
        prefixedQueueUrls <- Task.traverse(prefixedQueueNames)(sqs.operator.createQueue(_))
        resultList <- sqs.operator.listQueueUrls(prefix).toListL
      } yield {
        resultList.size shouldBe n
        resultList should contain theSameElementsAs prefixedQueueUrls.map(_.url)
      }
    }.runSyncUnsafe()
  }

  it can "send messages standard queue" in {
    val queueName = genQueueName.sample.get
    val message = Gen.identifier.map(_.take(10)).map(id => InboundMessage(id, None)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName)
        messageResponse <- sqs.producer.sendSingleMessage(message, queueUrl)

      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
    }.runSyncUnsafe()
  }

  it can "send messages to fifo queue, with deduplication and group id" in {
    val queueName = genQueueName.sample.get.map(_ + ".fifo") // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
    val message = Gen.identifier.map(_.take(10)).map(id => InboundMessage(id, Some(id))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        messageResponse <- sqs.producer.sendSingleMessage(message, queueUrl, Some("groupId1"))
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
    }.runSyncUnsafe()
  }

  /*"Change visibility" can "causes the message to be consumed again" in {
    val queueName = genFifoQueueName.sample.get.map(_ + ".fifo")
    val groupId = "groupId"
    val message = genInboundMessage(deduplicationId = None).sample.get
    val queueAttributes = Map(QueueAttributeName.FIFO_QUEUE -> "true",
      QueueAttributeName.CONTENT_BASED_DEDUPLICATION -> "true")
    val initialVisibilityTimeout = 100.seconds
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.operator.createQueue(queueName, attributes = queueAttributes)
        response1 <- sqs.producer.sendSingleMessage(message, queueUrl, Some(groupId))
        receivedMessages1 <- sqs.consumer.receiveSingleDeletable(queueUrl, 10, visibilityTimeout = initialVisibilityTimeout)
        messageWithinVisibilityTimeout1 <- sqs.consumer.receiveSingleDeletable(queueUrl, 1)
        messageWithinVisibilityTimeout2 <- sqs.consumer.receiveSingleDeletable(queueUrl, 1)
        messageWithinVisibilityTimeout3 <- sqs.consumer.receiveSingleDeletable(queueUrl, 1)
        changeVisibilityTimeoutResponse <- sqs.operator.changeMessageVisibility(queueUrl, visibilityTimeout = initialVisibilityTimeout)

      } yield {
        response1.md5OfMessageBody shouldBe md5Hex(message.body)
        receivedMessages.map(_.message.body()) should contain theSameElementsAs List(message).map(_.body)
      }
    }.runSyncUnsafe()
  }*/

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
