package monix.connect.sqs.domain.inbound

import monix.connect.sqs.MessageAttribute
import monix.connect.sqs.domain.QueueUrl
import software.amazon.awssdk.services.sqs.model.{MessageSystemAttributeNameForSends, SendMessageBatchRequestEntry, SendMessageRequest}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * Generic abstraction for queue messages, which will be implemented differently for standard
  * and fifo queue messages.
  *
  * This class has a private constructor as it should not be used directly,
  * use one of its implementations [[StandardMessage]] or [[FifoMessage]].
  *
  */
class InboundMessage private[sqs](body: String,
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
