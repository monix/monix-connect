package monix.connect.sqs

import monix.connect.sqs.domain.{QueueMessage, QueueName, QueueUrl}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.commons.codec.digest.DigestUtils.{md2Hex, md5Hex}
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.QueueAttributeName

import scala.concurrent.duration._

class SqsSuite extends AnyFlatSpecLike with Matchers with ScalaFutures with SqsFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)
  implicit val sqsClient: Sqs = Sqs.createUnsafe(asyncClient)
  val queueName: QueueName = genQueueName.sample.get

  //todo create a queue with attributes
  s"Sqs" can "create queue and check it exists" in {

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

  it can "create tags, add new ones and list them" in {
    val queueName = genQueueName.sample.get
    Sqs.fromConfig.use { sqs =>
      val initialTags = Map("mode" -> "difficult")
      val tagsByUrl = Map("environment" -> "test")

      for {
        queueUrl <- sqs.createQueue(queueName, tags = initialTags)
        _ <- sqs.tagQueue(queueUrl, tags = tagsByUrl) //tagging by url
        errorMessage <- sqs.tagQueue(QueueUrl("nonExistingQueue"), tags = tagsByUrl)
          .as(None).onErrorHandle(ex => Some(ex.getMessage))
        appliedTags <- sqs.listQueueTags(queueUrl)
      } yield {
        appliedTags shouldBe initialTags ++ tagsByUrl
        errorMessage.isDefined shouldBe true
        errorMessage.get.contains(nonExistingQueueErrorMsg) shouldBe true
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
        queueUrl <- sqs.createQueue(queueName, tags = initialTags)
        untagByUrl <- sqs.untagQueue(queueUrl, tagKeys = List(queueType)) >>
          sqs.listQueueTags(queueUrl)
        untagByName <- sqs.untagQueue(queueName, tagKeys = List(modeTagKey, environmentTagKey)) >> //tagging by queue name
          sqs.listQueueTags(queueUrl)
        errorMessage <- sqs.untagQueue(QueueUrl("nonExistingQueue"), tagKeys = List(dummyTagKey))
          .as(None).onErrorHandle(ex => Some(ex.getMessage))
      } yield {
        untagByUrl shouldBe initialTags.filterNot(kv => kv._1 == queueType)
        untagByName shouldBe Map(dummyTagKey -> "123")
        errorMessage.isDefined shouldBe true
        errorMessage.get.contains(nonExistingQueueErrorMsg) should contain
      }
    }.runSyncUnsafe()
  }

  it can "create attributes, add new ones and list them" in {
    val queueName = genQueueName.sample.get
    Sqs.fromConfig.use { sqs =>
      val initialAttributes = Map(QueueAttributeName.DELAY_SECONDS -> "60")
      val attributesByUrl =  Map(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS -> "12345")

      for {
        queueUrl <- sqs.createQueue(queueName, attributes = initialAttributes)
        _ <- sqs.setQueueAttributes(queueUrl, attributes = attributesByUrl)
        errorMessage <- sqs.setQueueAttributes(QueueUrl("randomUrl"), attributes = attributesByUrl)
          .as(None).onErrorHandle(ex => Some(ex.getMessage))
        attributes <- sqs.getQueueAttributes(queueUrl)
      } yield {
        val expectedAttributes = initialAttributes ++ attributesByUrl
        attributes.filter(kv => expectedAttributes.keys.toList.contains(kv._1)) should contain theSameElementsAs expectedAttributes
        errorMessage.isDefined shouldBe true
        errorMessage.get.contains(nonExistingQueueErrorMsg) shouldBe true
      }
    }.runSyncUnsafe()
  }

  it can "get a queue and queue url from its name" in {
    val queueName = genQueueName.sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        createdQueueUrl <- sqs.createQueue(queueName)
        queueUrl <- sqs.getQueueUrl(queueName)
        emptyQueueUrl <- sqs.getQueueUrl(QueueName("randomName123"))
      } yield {
        createdQueueUrl shouldBe QueueUrl(queueUrlPrefix(queueName.name))
        queueUrl shouldBe Some(QueueUrl(queueUrlPrefix(queueName.name)))
        emptyQueueUrl shouldBe None
      }
    }.runSyncUnsafe()
  }

  it can "delete queue by url" in {
    val queueName = genQueueName.sample.get

    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName)
        existsBefore <- sqs.existsQueue(queueName)
        existsAfterDelete <- sqs.deleteQueue(queueUrl) >> sqs.existsQueue(queueName)
        errorMessage <- sqs.deleteQueue(queueUrl).as(None).onErrorHandle(ex => Some(ex.getMessage))
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
        queueUrls <- Task.traverse(queueNames)(sqs.createQueue(_))
        fullQueueList <- sqs.listAllQueueUrls.toListL
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
        _ <- Task.traverse(nonPrefixedQueueNames)(sqs.createQueue(_))
        prefixedQueueUrls <- Task.traverse(prefixedQueueNames)(sqs.createQueue(_))
        resultList <- sqs.listQueueUrls(prefix).toListL
      } yield {
        resultList.size shouldBe n
        resultList should contain theSameElementsAs prefixedQueueUrls.map(_.url)
      }
    }.runSyncUnsafe()
  }

  it can "send messages standard queue" in {
    val queueName = genQueueName.sample.get
    val message = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, None)).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName)
        messageResponse <- sqs.sendMessage(message, queueUrl)

      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
      }
    }.runSyncUnsafe()
  }

  it can "send messages to fifo queue, with deduplication and group id" in {
    val queueName = genQueueName.sample.get.map(_ + ".fifo") // it must end with `.fifo` prefix, see https://github.com/aws/aws-sdk-php/issues/1331
    val message = Gen.identifier.map(_.take(10)).map(id => QueueMessage(id, Some(id))).sample.get
    Sqs.fromConfig.use { sqs =>
      for {
        queueUrl <- sqs.createQueue(queueName, attributes = Map(QueueAttributeName.FIFO_QUEUE -> "true"))
        messageResponse <- sqs.sendMessage(message, queueUrl, Some("groupId1"))
      } yield {
        messageResponse.md5OfMessageBody shouldBe md5Hex(message.body)
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
