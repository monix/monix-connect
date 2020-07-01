package monix.connect.sqs

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{
  AddPermissionRequest,
  AddPermissionResponse,
  CreateQueueRequest,
  CreateQueueResponse,
  DeleteMessageRequest,
  DeleteMessageResponse,
  DeleteQueueRequest,
  DeleteQueueResponse,
  GetQueueUrlRequest,
  GetQueueUrlResponse,
  ListQueuesRequest,
  ListQueuesResponse,
  ReceiveMessageRequest,
  ReceiveMessageResponse,
  SendMessageRequest,
  SendMessageResponse,
  SqsRequest,
  SqsResponse
}

trait SqsOp[In <: SqsRequest, Out <: SqsResponse] {
  def execute(sqsRequest: In)(implicit client: SqsAsyncClient): CompletableFuture[Out]
}

object SqsOp {

  implicit val addPermission = SqsOpFactory.build[AddPermissionRequest, AddPermissionResponse](_.addPermission(_))
  implicit val createQueue = SqsOpFactory.build[CreateQueueRequest, CreateQueueResponse](_.createQueue(_))
  implicit val deleteMessage = SqsOpFactory.build[DeleteMessageRequest, DeleteMessageResponse](_.deleteMessage(_))
  implicit val deleteQueue = SqsOpFactory.build[DeleteQueueRequest, DeleteQueueResponse](_.deleteQueue(_))
  implicit val getQueueUrl = SqsOpFactory.build[GetQueueUrlRequest, GetQueueUrlResponse](_.getQueueUrl(_))
  implicit val listQueues = SqsOpFactory.build[ListQueuesRequest, ListQueuesResponse](_.listQueues(_))
  implicit val receiveMessage = SqsOpFactory.build[ReceiveMessageRequest, ReceiveMessageResponse](_.receiveMessage(_))
  implicit val sendMessage = SqsOpFactory.build[SendMessageRequest, SendMessageResponse](_.sendMessage(_))

  private[this] object SqsOpFactory {
    def build[Req <: SqsRequest, Resp <: SqsResponse](
      operation: (SqsAsyncClient, Req) => CompletableFuture[Resp]): SqsOp[Req, Resp] = {
      new SqsOp[Req, Resp] {
        def execute(request: Req)(
          implicit
          client: SqsAsyncClient): CompletableFuture[Resp] = {
          operation(client, request)
        }
      }
    }
  }
}
