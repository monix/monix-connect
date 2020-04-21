package scalona.monix.connect.dynamodb

import cloriko.monix.connect.common.Transformer.Transformer
import monix.reactive.{Consumer, Observable, Observer}
import monix.execution.Ack
import monix.eval.Task
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

import scala.jdk.FutureConverters._

object DynamoDb {

  def consumer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient = DynamoDbClient()): Consumer[In, Task[Out]] = {
    Consumer.create[In, Task[Out]] { (_, _, callback) =>
      new Observer.Sync[In] {
        private var dynamoDbResponse: Task[Out] = _

        def onNext(dynamoDbRequest: In): Ack = {
          dynamoDbResponse = Task.fromFuture(dynamoDbOp.execute(dynamoDbRequest).asScala)
          monix.execution.Ack.Continue
        }

        def onComplete(): Unit = {
          callback.onSuccess(dynamoDbResponse)
        }

        def onError(ex: Throwable): Unit = {
          callback.onError(ex)
        }
      }
    }
  }

  def transformer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient = DynamoDbClient()): Transformer[In, Task[Out]] = { inObservable: Observable[In] =>
    inObservable.map(in => Task.fromFuture(dynamoDbOp.execute(in).asScala))
  }
}
