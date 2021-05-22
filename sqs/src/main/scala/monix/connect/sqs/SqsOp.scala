/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
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
import software.amazon.awssdk.services.sqs.model.{
  AddPermissionRequest,
  AddPermissionResponse,
  ChangeMessageVisibilityRequest,
  ChangeMessageVisibilityResponse,
  CreateQueueRequest,
  CreateQueueResponse,
  DeleteMessageRequest,
  DeleteMessageResponse,
  DeleteQueueRequest,
  DeleteQueueResponse,
  GetQueueAttributesRequest,
  GetQueueAttributesResponse,
  GetQueueUrlRequest,
  GetQueueUrlResponse,
  ListDeadLetterSourceQueuesRequest,
  ListDeadLetterSourceQueuesResponse,
  ListQueueTagsRequest,
  ListQueueTagsResponse,
  ListQueuesRequest,
  ListQueuesResponse,
  PurgeQueueRequest,
  PurgeQueueResponse,
  ReceiveMessageRequest,
  ReceiveMessageResponse,
  RemovePermissionRequest,
  RemovePermissionResponse,
  SendMessageBatchRequest,
  SendMessageBatchResponse,
  SendMessageRequest,
  SendMessageResponse,
  SetQueueAttributesRequest,
  SetQueueAttributesResponse,
  SqsRequest,
  SqsResponse,
  TagQueueRequest,
  TagQueueResponse,
  UntagQueueRequest,
  UntagQueueResponse
}

trait SqsOp[Request <: SqsRequest, Response <: SqsResponse] {
  def execute(sqsRequest: Request)(implicit client: SqsAsyncClient): Task[Response]
}

object SqsOp {

  implicit val addPermission: SqsOp[AddPermissionRequest, AddPermissionResponse] =
    SqsOp[AddPermissionRequest, AddPermissionResponse](_.addPermission(_))
  implicit val changeMessageVisibility: SqsOp[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResponse] =
    SqsOp[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResponse](_.changeMessageVisibility(_))
  implicit val createQueue: SqsOp[CreateQueueRequest, CreateQueueResponse] =
    SqsOp[CreateQueueRequest, CreateQueueResponse](_.createQueue(_))
  implicit val deleteMessage: SqsOp[DeleteMessageRequest, DeleteMessageResponse] =
    SqsOp[DeleteMessageRequest, DeleteMessageResponse](_.deleteMessage(_))
  implicit val deleteQueue: SqsOp[DeleteQueueRequest, DeleteQueueResponse] =
    SqsOp[DeleteQueueRequest, DeleteQueueResponse](_.deleteQueue(_))
  implicit val getQueueUrl: SqsOp[GetQueueUrlRequest, GetQueueUrlResponse] =
    SqsOp[GetQueueUrlRequest, GetQueueUrlResponse](_.getQueueUrl(_))
  implicit val getQueueAttributes: SqsOp[GetQueueAttributesRequest, GetQueueAttributesResponse] =
    SqsOp[GetQueueAttributesRequest, GetQueueAttributesResponse](_.getQueueAttributes(_))
  implicit val listDeadLetter: SqsOp[ListDeadLetterSourceQueuesRequest, ListDeadLetterSourceQueuesResponse] =
    SqsOp[ListDeadLetterSourceQueuesRequest, ListDeadLetterSourceQueuesResponse](_.listDeadLetterSourceQueues(_))
  implicit val listQueues: SqsOp[ListQueuesRequest, ListQueuesResponse] =
    SqsOp[ListQueuesRequest, ListQueuesResponse](_.listQueues(_))
  implicit val listQueueTags: SqsOp[ListQueueTagsRequest, ListQueueTagsResponse] =
    SqsOp[ListQueueTagsRequest, ListQueueTagsResponse](_.listQueueTags(_))
  implicit val purgeQueue: SqsOp[PurgeQueueRequest, PurgeQueueResponse] =
    SqsOp[PurgeQueueRequest, PurgeQueueResponse](_.purgeQueue(_))
  implicit val receiveMessage: SqsOp[ReceiveMessageRequest, ReceiveMessageResponse] =
    SqsOp[ReceiveMessageRequest, ReceiveMessageResponse](_.receiveMessage(_))
  implicit val removePermission: SqsOp[RemovePermissionRequest, RemovePermissionResponse] =
    SqsOp[RemovePermissionRequest, RemovePermissionResponse](_.removePermission(_))
  implicit val sendMessage: SqsOp[SendMessageRequest, SendMessageResponse] =
    SqsOp[SendMessageRequest, SendMessageResponse](_.sendMessage(_))
  implicit val sendMessageBatch: SqsOp[SendMessageBatchRequest, SendMessageBatchResponse] =
    SqsOp[SendMessageBatchRequest, SendMessageBatchResponse](_.sendMessageBatch(_))
  implicit val setQueueAttributes: SqsOp[SetQueueAttributesRequest, SetQueueAttributesResponse] =
    SqsOp[SetQueueAttributesRequest, SetQueueAttributesResponse](_.setQueueAttributes(_))
  implicit val tagQueue: SqsOp[TagQueueRequest, TagQueueResponse] =
    SqsOp[TagQueueRequest, TagQueueResponse](_.tagQueue(_))
  implicit val untagQueue: SqsOp[UntagQueueRequest, UntagQueueResponse] =
    SqsOp[UntagQueueRequest, UntagQueueResponse](_.untagQueue(_))

  private def apply[Request <: SqsRequest, Response <: SqsResponse](
    operation: (SqsAsyncClient, Request) => CompletableFuture[Response]): SqsOp[Request, Response] = {
    new SqsOp[Request, Response] {
      def execute(request: Request)(implicit sqsAsyncClient: SqsAsyncClient): Task[Response] = {
        Task.from(operation(sqsAsyncClient, request))
      }
    }
  }

}
