package monix.connect.sqs

import monix.eval.Task
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Callback, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.Consumer
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SqsRequest, SqsResponse}

import scala.concurrent.Future

private[sqs] class SqsSubscriber[In <: SqsRequest, Out <: SqsResponse]()(
  implicit
  sqsOp: SqsOp[In, Out],
  client: SqsAsyncClient)
  extends Consumer[In, Out] {

  override def createSubscriber(cb: Callback[Throwable, Out], s: Scheduler): (Subscriber[In], AssignableCancelable) = {
    val sub = new Subscriber[In] {

      implicit val scheduler = s
      private var sqsResponse: Task[Out] = _

      def onNext(sqsRequest: In): Future[Ack] = {
        sqsResponse = Task.from(sqsOp.execute(sqsRequest))

        sqsResponse.onErrorRecover { case _ => monix.execution.Ack.Stop }
          .map(_ => monix.execution.Ack.Continue)
          .runToFuture
      }

      def onComplete(): Unit = {
        sqsResponse.runAsync(cb)
      }

      def onError(ex: Throwable): Unit = {
        cb.onError(ex)
      }
    }
    (sub, AssignableCancelable.single())
  }

}
