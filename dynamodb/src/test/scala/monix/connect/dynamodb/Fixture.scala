package monix.connect.dynamodb

import monix.catnap.MVar
import monix.eval.Task
import monix.execution.Scheduler
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse}

import java.util.concurrent.CompletableFuture
import scala.concurrent.Future
import scala.jdk.FutureConverters._

trait Fixture {

  val client: DynamoDbAsyncClient = new DynamoDbAsyncClient{
    override def serviceName(): String = ???
    override def close(): Unit = ???
  }

  val req = GetItemRequest.builder().build()
  val resp = GetItemResponse.builder().build()

  def withOperationStub(f: Int => Task[GetItemResponse])(implicit s: Scheduler) =
    new DynamoDbOp[GetItemRequest, GetItemResponse] {
      override def apply(dynamoDbRequest: GetItemRequest)(implicit client: DynamoDbAsyncClient): Task[GetItemResponse] =
        Task.defer(Task.from(execute(dynamoDbRequest)))
      val counterMvarTask: Task[MVar[Task, Int]] = MVar[Task].of(1).memoize
      def incWithF(): Task[GetItemResponse] = counterMvarTask.flatMap(counterMvar => counterMvar.take.flatMap(value =>
        counterMvar.put(value + 1) >> f(value)))

      override def execute(dynamoDbRequest: GetItemRequest)(implicit client: DynamoDbAsyncClient): CompletableFuture[GetItemResponse] = {
        incWithF.runToFuture.flatMap(r =>
          Future.successful(r)).asJava.toCompletableFuture
      }
    }


}
