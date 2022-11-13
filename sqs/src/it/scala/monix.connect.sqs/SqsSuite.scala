package monix.connect.sqs

import monix.connect.aws.auth.MonixAwsConf
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskTest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.KebabCase

class SqsSuite extends AsyncFlatSpec with MonixTaskTest with Matchers with BeforeAndAfterAll with SqsITFixture {

  override implicit val scheduler = Scheduler.io("sqs-suite")

  s"$Sqs" can "be created from config file" in {
    Sqs.fromConfig().use { sqs =>
      for {
        fifoQueueName <- Task.from(genFifoQueueName)
        createdQueueUrl <- sqs.operator.createQueue(fifoQueueName)
        getQueueUrl <- sqs.operator.getQueueUrl(fifoQueueName)
      } yield {
        createdQueueUrl shouldEqual getQueueUrl
      }
    }.assertNoException
  }

  it can "be created from config file with an specific naming convention" in {
    Sqs.fromConfig(KebabCase).use { sqs =>
      for {
        fifoQueueName <- Task.from(genFifoQueueName)
        createdQueueUrl <- sqs.operator.createQueue(fifoQueueName)
        getQueueUrl <- sqs.operator.getQueueUrl(fifoQueueName)
      } yield {
        createdQueueUrl shouldEqual getQueueUrl
      }
    }.assertNoException
  }

  it can "be created from monix aws conf" in {

    MonixAwsConf.load().memoizeOnSuccess.flatMap {
      Sqs.fromConfig(_).use { sqs =>
        for {
          fifoQueueName <- Task.from(genFifoQueueName)
          createdQueueUrl <- sqs.operator.createQueue(fifoQueueName)
          getQueueUrl <- sqs.operator.getQueueUrl(fifoQueueName)
        } yield {
          createdQueueUrl shouldEqual getQueueUrl
        }
      }
    }.assertNoException
  }

  it can "be created from monix aws conf task" in {

    val monixAwsConf = MonixAwsConf.load()
    Sqs.fromConfig(monixAwsConf).use { sqs =>
      for {
        fifoQueueName <- Task.from(genFifoQueueName)
        createdQueueUrl <- sqs.operator.createQueue(fifoQueueName)
        getQueueUrl <- sqs.operator.getQueueUrl(fifoQueueName)
      } yield {
        createdQueueUrl shouldEqual getQueueUrl
      }
    }.assertNoException
  }

}
