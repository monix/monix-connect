package monix.connect.sqs

import monix.eval.Task
import monix.reactive.{Consumer, Observable}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{SqsRequest, SqsResponse}

object Sqs {

  def consumer[In <: SqsRequest, Out <: SqsResponse](
    implicit sqsOp: SqsOp[In, Out],
    client: SqsAsyncClient): Consumer[In, Out] = new SqsSubscriber

  def transformer[In <: SqsRequest, Out <: SqsResponse](
    implicit
    sqsOp: SqsOp[In, Out],
    client: SqsAsyncClient): Observable[In] => Observable[Task[Out]] = { inObservable: Observable[In] =>
    inObservable.map(in => Task.from(sqsOp.execute(in)))
  }
}
