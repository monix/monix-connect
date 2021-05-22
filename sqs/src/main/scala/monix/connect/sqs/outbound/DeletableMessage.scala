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

package monix.connect.sqs.outbound

import monix.connect.sqs.SqsOp
import monix.connect.sqs.domain.QueueUrl
import monix.eval.Task
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{DeleteMessageRequest, Message}

/**
  * Represents a message consumed with manual delete, meaning that it provides
  * control over when the message is considered processed and wants
  * to be deleted from the source queue so that the next message can be consumed.
  *
  * And that functionality is exposed by the [[deleteFromQueue]] method.
  */
class DeletableMessage private[sqs] (override val queueUrl: QueueUrl, override protected val message: Message)(
  implicit asyncClient: SqsAsyncClient)
  extends ReceivedMessage(queueUrl, message) {

  def deleteFromQueue(): Task[Unit] = {
    val deleteMessageRequest = DeleteMessageRequest.builder
      .queueUrl(queueUrl.url)
      .receiptHandle(message.receiptHandle)
      .build
    SqsOp.deleteMessage.execute(deleteMessageRequest)(asyncClient).void
  }

}
