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

package monix.connect.sqs.consumer

import monix.connect.sqs.SqsOp
import monix.connect.sqs.domain.QueueUrl
import monix.eval.Task
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{ChangeMessageVisibilityRequest, Message, MessageSystemAttributeName}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/** Represents the generic implementation of a message consumed from a specific queue. */
class ConsumedMessage private[sqs](val queueUrl: QueueUrl, protected val message: Message)(
  implicit asyncClient: SqsAsyncClient) {

  val body: String = message.body()
  val messageId: String = message.messageId()
  val attributes: Map[MessageSystemAttributeName, String] = message.attributes().asScala.toMap
  val md5OfBody: String = message.md5OfBody()

  def changeVisibilityTimeout(timeout: FiniteDuration): Task[Unit] = {
    val changeMessageVisibilityRequest = ChangeMessageVisibilityRequest.builder
      .queueUrl(queueUrl.url)
      .receiptHandle(message.receiptHandle)
      .visibilityTimeout(timeout.toSeconds.toInt)
      .build
    SqsOp.changeMessageVisibility.execute(changeMessageVisibilityRequest)(asyncClient).void
  }

}
