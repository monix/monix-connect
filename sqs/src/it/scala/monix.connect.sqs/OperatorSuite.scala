package monix.connect.sqs

import monix.connect.sqs.domain.QueueUrl
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.exceptions.DummyException
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.sqs.model.{QueueAttributeName, QueueDoesNotExistException}

import scala.concurrent.duration.DurationInt

class OperatorSuite extends AsyncFlatSpec with MonixTaskTest with Matchers with BeforeAndAfterAll with SqsITFixture {

  override implicit val scheduler = Scheduler.io("sqs-operator-suite")

  s"Sqs" can "create queue and check it exists" in {
    val queueName = randomQueueName
    for {
      sqs <- unsafeSqsAsyncClient
      existedBefore <- sqs.operator.existsQueue(queueName)
      _ <- sqs.operator.createQueue(queueName)
      existsAfter <- sqs.operator.existsQueue(queueName)
    } yield {
      existedBefore shouldBe false
      existsAfter shouldBe true
    }
  }

  it must "fail when getting the url of a non existing queue" in {
   unsafeSqsAsyncClient.flatMap(_.operator.getQueueUrl(randomQueueName))
     .attempt.asserting{ queueUrl =>
     queueUrl.isLeft shouldBe true
     queueUrl.toTry.failed.get shouldBe a[QueueDoesNotExistException]
   }
  }

  it can "purge a queue" in {
    val messages = Gen.listOfN(100, genStandardMessage).sample.get
    for {
      sqs <- unsafeSqsAsyncClient
      queueUrl <- sqs.operator.createQueue(randomQueueName)
      _ <- sqs.producer.sendParBatch(messages, queueUrl)
      receiveBeforePurge <- sqs.consumer.receiveSingleAutoDelete(queueUrl, maxMessages = 5)
      _ <- sqs.operator.purgeQueue(queueUrl) >> Task.sleep(2.seconds)
      receiveAfterPurge <- sqs.consumer.receiveSingleAutoDelete(queueUrl)
    } yield {
      receiveBeforePurge.size shouldBe 5
      receiveAfterPurge shouldBe List.empty
    }
  }

  it can "create tags, add new ones and list them" in {
      val initialTags = Map("mode" -> "difficult")
      val tagsByUrl = Map("environment" -> "test")

      for {
        sqs <- unsafeSqsAsyncClient
        queueUrl <- sqs.operator.createQueue(randomQueueName, tags = initialTags)
        _ <- sqs.operator.tagQueue(queueUrl, tags = tagsByUrl) //tagging by url
        nonExistingQueue <- sqs.operator.tagQueue(QueueUrl("nonExistingQueue"), tags = tagsByUrl).attempt
        appliedTags <- sqs.operator.listQueueTags(queueUrl)
      } yield {
        appliedTags shouldBe initialTags ++ tagsByUrl
        nonExistingQueue.isLeft shouldBe true
        nonExistingQueue.swap.getOrElse(DummyException("failed")).getMessage should include(invalidRequestErrorMsg)
      }
  }

  it can "remove tags from a queue" in {
      val queueType = "type"
      val modeTagKey = "mode"
      val environmentTagKey = "environment"
      val dummyTagKey = "dummy"
      val initialTags = Map(queueType -> "1", modeTagKey -> "difficult",
        environmentTagKey -> "test", dummyTagKey -> "123")
      for {
        sqs <- unsafeSqsAsyncClient
        queueUrl <- sqs.operator.createQueue(randomQueueName, tags = initialTags)
        untagByUrl <- sqs.operator.untagQueue(queueUrl, tagKeys = List(queueType)) >>
          sqs.operator.listQueueTags(queueUrl)
        untagByName <- sqs.operator.untagQueue(queueUrl, tagKeys = List(modeTagKey, environmentTagKey)) >> //tagging by queue name
          sqs.operator.listQueueTags(queueUrl)
        nonExistingQueue <- sqs.operator.untagQueue(QueueUrl("nonExistingQueue"), tagKeys = List(dummyTagKey)).attempt
      } yield {
        untagByUrl shouldBe initialTags.filterNot(kv => kv._1 == queueType)
        untagByName shouldBe Map(dummyTagKey -> "123")
        nonExistingQueue.isLeft shouldBe true
        nonExistingQueue.swap.getOrElse(DummyException("failed")).getMessage should include(invalidRequestErrorMsg)
      }
  }

  it can "create attributes, add new ones and list them" in {
      val initialAttributes = Map(QueueAttributeName.DELAY_SECONDS -> "60")
      val attributesByUrl =  Map(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS -> "12345")
      for {
        sqs <- unsafeSqsAsyncClient
        queueUrl <- sqs.operator.createQueue(randomQueueName, attributes = initialAttributes)
        _ <- sqs.operator.setQueueAttributes(queueUrl, attributes = attributesByUrl)
        nonExistingQueue <- sqs.operator.setQueueAttributes(QueueUrl("randomUrl"), attributes = attributesByUrl).attempt
        attributes <- sqs.operator.getQueueAttributes(queueUrl)
      } yield {
        val expectedAttributes = initialAttributes ++ attributesByUrl
        attributes.filter(kv => expectedAttributes.keys.toList.contains(kv._1)) should contain theSameElementsAs expectedAttributes
        nonExistingQueue.isLeft shouldBe true
        nonExistingQueue.swap.getOrElse(DummyException("failed")).getMessage should include(invalidRequestErrorMsg)
      }
  }

  it can "get a queue and queue url from its name" in {
    val queueName = randomQueueName
      for {
        sqs <- unsafeSqsAsyncClient
        createdQueueUrl <- sqs.operator.createQueue(queueName)
        queueUrl <- sqs.operator.getQueueUrl(queueName)
        nonExistingQueue <- sqs.operator.getQueueUrl(randomQueueName).attempt
      } yield {
        createdQueueUrl shouldBe QueueUrl(queueUrlPrefix(queueName.name))
        queueUrl shouldBe QueueUrl(queueUrlPrefix(queueName.name))
        nonExistingQueue.isLeft shouldBe true
        nonExistingQueue.swap.getOrElse(DummyException("failed")).isInstanceOf[QueueDoesNotExistException] shouldBe true
      }
  }

  it can "delete queue by url" in {
    val queueName = randomQueueName
    for {
        sqs <- unsafeSqsAsyncClient
        queueUrl <- sqs.operator.createQueue(queueName)
        existsBefore <- sqs.operator.existsQueue(queueName)
        existsAfterDelete <- sqs.operator.deleteQueue(queueUrl) >> sqs.operator.existsQueue(queueName)
        deleteWithError <- sqs.operator.deleteQueue(queueUrl).attempt
      } yield {
        existsBefore shouldBe true
        existsAfterDelete shouldBe false
        deleteWithError.isLeft shouldBe true
        deleteWithError.toTry.failed.get shouldBe a[QueueDoesNotExistException]
      }
  }

  it can "list all queue urls" in {
    for {
      sqs <- unsafeSqsAsyncClient
      queueNames <- Task.from(Gen.listOfN(10, genFifoQueueName))
      queueUrls <- Task.traverse(queueNames)(a => sqs.operator.createQueue(a))
      fullQueueList <- sqs.operator.listQueueUrls().toListL
    } yield {
      fullQueueList.size should be >= queueNames.size
      fullQueueList should contain allElementsOf queueUrls
    }
  }

  it can "list queue by the name prefix name" in {
    val prefix = Gen.identifier.map(id => s"test-${id.take(5)}").sample.get
    val nonPrefixedQueueNames = Gen.listOfN(10, genFifoQueueName).sample.get
    val prefixedQueueNames = Gen.listOfN(6, genFifoQueueName.map(_.map(prefix + _))).sample.get

      for {
        sqs <- unsafeSqsAsyncClient
        _ <- Task.traverse(nonPrefixedQueueNames)(sqs.operator.createQueue(_))
        prefixedQueueUrls <- Task.traverse(prefixedQueueNames)(sqs.operator.createQueue(_))
        resultList <- sqs.operator.listQueueUrls(Some(prefix)).toListL
      } yield {
        resultList.size shouldBe prefixedQueueNames.size
        resultList should contain theSameElementsAs prefixedQueueUrls
      }
  }

  // FIXME: This operation is not supported in local elasticMq
  //it can "list all dlq queue urls" in {
  //  val queueName = genQueueName.sample.get
  //  val dlQueueName = genQueueName.sample.get
  //  Sqs.fromConfig.use { sqs =>
  //    for {
  //      queueUrl <- sqs.operator.createQueue(queueName)
  //      dlQueueUrl <- sqs.operator.createQueue(dlQueueName)
  //      dlQueueArn <- sqs.operator.getQueueAttributes(dlQueueUrl).map(_.get(QueueAttributeName.QUEUE_ARN))
  //      _ <- sqs.operator.setQueueAttributes(queueUrl, attributes = dlQueueArn.map(dlqRedrivePolicyAttr).getOrElse(Map.empty))
  //    } yield {
  //      //dlQueueList.size shouldBe 1
  //    }
  //  }.runSyncUnsafe()
  //}

}
