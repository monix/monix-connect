/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.sqs

import monix.eval.Task

import java.util.concurrent.CompletableFuture
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{AddPermissionRequest, AddPermissionResponse, ChangeMessageVisibilityRequest, ChangeMessageVisibilityResponse, CreateQueueRequest, CreateQueueResponse, DeleteMessageRequest, DeleteMessageResponse, DeleteQueueRequest, DeleteQueueResponse, GetQueueUrlRequest, GetQueueUrlResponse, ListQueuesRequest, ListQueuesResponse, ReceiveMessageRequest, ReceiveMessageResponse, SendMessageBatchRequest, SendMessageBatchResponse, SendMessageRequest, SendMessageResponse, SqsRequest, SqsResponse}

trait SqsOp[Request <: SqsRequest, Response <: SqsResponse] {
  def execute(sqsRequest: Request)(implicit client: SqsAsyncClient): Task[Response]
}

object SqsOp {

  implicit val addPermission: SqsOp[AddPermissionRequest, AddPermissionResponse] =
    SqsOpFactory.build[AddPermissionRequest, AddPermissionResponse](_.addPermission(_))
  implicit val createQueue: SqsOp[CreateQueueRequest, CreateQueueResponse] = SqsOpFactory.build[CreateQueueRequest, CreateQueueResponse](_.createQueue(_))
  implicit val deleteMessage: SqsOp[DeleteMessageRequest, DeleteMessageResponse] = SqsOpFactory.build[DeleteMessageRequest, DeleteMessageResponse](_.deleteMessage(_))
  implicit val deleteQueue: SqsOp[DeleteQueueRequest, DeleteQueueResponse] = SqsOpFactory.build[DeleteQueueRequest, DeleteQueueResponse](_.deleteQueue(_))
  implicit val getQueueUrl: SqsOp[GetQueueUrlRequest, GetQueueUrlResponse] = SqsOpFactory.build[GetQueueUrlRequest, GetQueueUrlResponse](_.getQueueUrl(_))
  implicit val listQueues: SqsOp[ListQueuesRequest, ListQueuesResponse] = SqsOpFactory.build[ListQueuesRequest, ListQueuesResponse](_.listQueues(_))
  implicit val receiveMessage: SqsOp[ReceiveMessageRequest, ReceiveMessageResponse] = SqsOpFactory.build[ReceiveMessageRequest, ReceiveMessageResponse](_.receiveMessage(_))
  implicit val sendMessage: SqsOp[SendMessageRequest, SendMessageResponse] = SqsOpFactory.build[SendMessageRequest, SendMessageResponse](_.sendMessage(_))
  implicit val sendMessageBatch: SqsOp[SendMessageBatchRequest, SendMessageBatchResponse] = SqsOpFactory.build[SendMessageBatchRequest, SendMessageBatchResponse](_.sendMessageBatch(_))
  implicit val changeMessageVisibility: SqsOp[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResponse] =
    SqsOpFactory.build[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResponse](_.changeMessageVisibility(_))

  private[this] object SqsOpFactory {
    def build[Request <: SqsRequest, Response <: SqsResponse](
      operation: (SqsAsyncClient, Request) => CompletableFuture[Response]): SqsOp[Request, Response] = {
      new SqsOp[Request, Response] {
        def execute(request: Request)(
          implicit sqsAsyncClient: SqsAsyncClient): Task[Response] = {
          Task.defer(
            Task.from(operation(sqsAsyncClient, request))
          )
        }
      }
    }
  }
}
