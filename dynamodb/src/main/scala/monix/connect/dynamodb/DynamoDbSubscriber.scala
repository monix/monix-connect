package monix.connect.dynamodb

import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.{Consumer, Observer}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{DynamoDbRequest, DynamoDbResponse}

import scala.concurrent.Future
import scala.jdk.FutureConverters._

private[dynamodb] class DynamoDbSubscriber[In <: DynamoDbRequest, Out <: DynamoDbResponse]()(implicit dynamoDbOp: DynamoDbOp[In, Out],
                                                                                             client: DynamoDbAsyncClient) extends Consumer[In, Out] {

  override def createSubscriber(cb: Callback[Throwable, Out], s: Scheduler): (Subscriber[In], AssignableCancelable) = {
    val sub = new Subscriber[In] {

      implicit val scheduler = s
      private var dynamoDbResponse: Task[Out] = _

      def onNext(dynamoDbRequest: In): Future[Ack] = {
        dynamoDbResponse = Task.fromFuture(
          dynamoDbOp
            .execute(dynamoDbRequest)
            .asScala)

        dynamoDbResponse.onErrorRecover { case _ => monix.execution.Ack.Stop }
          .map(_ => monix.execution.Ack.Continue)
          .runToFuture
      }

      def onComplete(): Unit = {
        dynamoDbResponse.runAsync(cb)
      }

      def onError(ex: Throwable): Unit = {
        cb.onError(ex)
      }
    }
    (sub, AssignableCancelable.single())
  }

}
