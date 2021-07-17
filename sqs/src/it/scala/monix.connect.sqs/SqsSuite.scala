package monix.connect.sqs

import monix.connect.aws.auth.MonixAwsConf
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import pureconfig.KebabCase

class SqsSuite extends AnyFlatSpecLike with Matchers with BeforeAndAfterEach with SqsITFixture {

  s"$Sqs" can "be created from config file" in {

    Sqs.fromConfig.use { sqs =>
      for {
        createdQueueUrl <- sqs.operator.createQueue(fifoQueueName)
        getQueueUrl <- sqs.operator.getQueueUrl(fifoQueueName)
      } yield {
        createdQueueUrl shouldEqual getQueueUrl
      }
    }.runSyncUnsafe()
  }

  it can "be created from config file with an specific naming convention" in {

    Sqs.fromConfig(KebabCase).use { sqs =>
      for {
        createdQueueUrl <- sqs.operator.createQueue(fifoQueueName)
        getQueueUrl <- sqs.operator.getQueueUrl(fifoQueueName)
      } yield {
        createdQueueUrl shouldEqual getQueueUrl
      }
    }.runSyncUnsafe()
  }

  it can "be created from monix aws conf" in {

    MonixAwsConf.load().memoizeOnSuccess.flatMap {
      Sqs.fromConfig(_).use { sqs =>
        for {
          createdQueueUrl <- sqs.operator.createQueue(fifoQueueName)
          getQueueUrl <- sqs.operator.getQueueUrl(fifoQueueName)
        } yield {
          createdQueueUrl shouldEqual getQueueUrl
        }
      }
    }.runSyncUnsafe()
  }

  it can "be created from monix aws conf task" in {

    val monixAwsConf = MonixAwsConf.load()
    Sqs.fromConfig(monixAwsConf).use { sqs =>
      for {
        createdQueueUrl <- sqs.operator.createQueue(fifoQueueName)
        getQueueUrl <- sqs.operator.getQueueUrl(fifoQueueName)
      } yield {
        createdQueueUrl shouldEqual getQueueUrl
      }
    }.runSyncUnsafe()

  }

}
