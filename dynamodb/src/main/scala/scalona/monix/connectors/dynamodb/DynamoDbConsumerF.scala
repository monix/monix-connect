package scalona.monix.connectors.dynamodb

import monix.eval.Task
import monix.execution.Ack
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{Consumer, MulticastStrategy, Observable, Observer}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

import scala.jdk.FutureConverters._

class DynamoDbConsumerF()(implicit client: DynamoDbAsyncClient) {

  def build[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    implicit dynamoDbOp: DynamoDbOp[In, Out]): Consumer[In, Task[Out]] = {
    Consumer.create[In, Task[Out]] { (scheduler, _, callback) =>
      new Observer.Sync[In] {
       private var response: Task[Out] = _

        def onNext(dynamoDbRequest: In): Ack = {
          val dynamoDbResponse: Task[Out] = Task.fromFuture(dynamoDbOp.execute(dynamoDbRequest).asScala)
          response = dynamoDbResponse
          monix.execution.Ack.Continue
        }

        def onComplete(): Unit = {
          callback.onSuccess(response)
        }

        def onError(ex: Throwable): Unit = {
          callback.onError(ex)
        }
      }
    }
  }
}


object DynamoDbConsumerF {
  def apply()(implicit client: DynamoDbAsyncClient = DynamoDbClient()): DynamoDbConsumerF = new DynamoDbConsumerF()
}
