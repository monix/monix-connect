package scalona.monix.connectors.dynamodb

import monix.reactive.{Consumer, MulticastStrategy, Observable, Observer, OverflowStrategy}
import monix.execution.Ack
import monix.eval.Task
import monix.reactive.subjects.ConcurrentSubject
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

import scala.jdk.FutureConverters._
import scala.concurrent.Future

class DynamoDbConsumer()(implicit client: DynamoDbAsyncClient) {

  def build[In <: DynamoDbRequest, Out <: DynamoDbResponse](implicit dynamoDbOp: DynamoDbOp[In, Out]): Consumer[In, Observable[Task[Out]]] = {
    Consumer.create[In, Observable[Task[Out]]] { (scheduler , _, callback) =>
      new Observer.Sync[In] {
        val subject: ConcurrentSubject[Task[Out], Task[Out]] =
          ConcurrentSubject[Task[Out]](MulticastStrategy.replay)(scheduler)
        // private var dynamoDbResponse: List[Task[Out]] = _

        def onNext(dynamoDbRequest: In): Ack = {
          val dynamoDbResponse: Task[Out] = Task.fromFuture(dynamoDbOp.execute(dynamoDbRequest).asScala)
          subject.onNext(dynamoDbResponse)
          monix.execution.Ack.Continue
        }

        def onComplete(): Unit = {
          callback.onSuccess(subject)
        }

        def onError(ex: Throwable): Unit = {
          callback.onError(ex)
        }
      }
    }
  }
}

object DynamoDbConsumer {
  def apply()(implicit client: DynamoDbAsyncClient = DynamoDbClient()): DynamoDbConsumer = new DynamoDbConsumer()
}
