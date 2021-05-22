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

package monix.connect.sqs.inbound

import monix.connect.sqs.MessageAttribute
import monix.connect.sqs.domain.QueueUrl
import software.amazon.awssdk.services.sqs.model.{
  MessageSystemAttributeNameForSends,
  SendMessageBatchRequestEntry,
  SendMessageRequest
}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * Generic abstraction for queue messages, which will be implemented differently for standard
  * and fifo queue messages.
  *
  * This class has a private constructor as it should not be used directly,
  * use one of its implementations [[StandardMessage]] or [[FifoMessage]].
  *
  * The reason for that distinction is to make the api easier and transparent
  * to use on the user side, because different queues expect different attributes,
  * for example a [[FifoMessage]] has `groupId` and `deduplicaitonId`, but that
  * is not valid for standard queues.
  * On the other hand [[StandardMessage]]s accept `delayDurations`, which are not
  * allowed in fifo implementation.
  * Thus it enforces the user to use the right attributes for their use case.
  *
  */
class InboundMessage private[sqs] (
  body: String,
  groupId: Option[String],
  deduplicationId: Option[String] = Option.empty,
  messageAttributes: Map[String, MessageAttribute] = Map.empty,
  awsTraceHeader: Option[MessageAttribute] = Option.empty,
  delayDuration: Option[FiniteDuration] = None) {

  private[sqs] def toMessageRequest(queueUrl: QueueUrl): SendMessageRequest = {
    val builder = SendMessageRequest.builder.messageBody(body).queueUrl(queueUrl.url)
    delayDuration.map(delay => builder.delaySeconds(delay.toSeconds.toInt))
    groupId.map(builder.messageGroupId)
    deduplicationId.map(builder.messageDeduplicationId)
    builder.messageAttributes(messageAttributes.map { case (k, v) => (k, v.toAttrValue) }.asJava)
    awsTraceHeader.map { attr =>
      builder.messageSystemAttributes(
        Map(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER -> attr.toSystemAttrValue).asJava
      )
    }
    builder.build
  }

  private[sqs] def toMessageBatchEntry(batchId: String): SendMessageBatchRequestEntry = {
    val builder = SendMessageBatchRequestEntry.builder
      .messageBody(body)
      .id(batchId)
    deduplicationId.map(builder.messageDeduplicationId)
    groupId.map(builder.messageGroupId)
    delayDuration.map(delay => builder.delaySeconds(delay.toMillis.toInt))
    builder.messageAttributes(messageAttributes.map { case (k, v) => (k, v.toAttrValue) }.asJava)
    awsTraceHeader.map { attr =>
      builder.messageSystemAttributes(
        Map(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER -> attr.toSystemAttrValue).asJava
      )
    }
    builder.build
  }

}
