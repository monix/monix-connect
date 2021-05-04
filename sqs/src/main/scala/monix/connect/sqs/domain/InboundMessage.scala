package monix.connect.sqs.domain

import monix.connect.sqs.MessageAttribute
import software.amazon.awssdk.services.sqs.model.{MessageSystemAttributeNameForSends, SendMessageBatchRequestEntry, SendMessageRequest}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * Simple case class encodes the queueUrl.
  * The reason for this class to exist is to have
  * a proper distinction between `queueUrl` and the `queueName`,
  * so that they can not be confused on the method signatures.
  */
class InboundMessage private[sqs](body: String,
                                  groupId: Option[String],
                                  deduplicationId: Option[String] = Option.empty,
                                  messageAttributes: Map[String, MessageAttribute] = Map.empty,
                                  awsTraceHeader: Option[MessageAttribute] = Option.empty) {

  private[sqs] def toMessageRequest(queueUrl: QueueUrl,
                                          delayDuration: Option[FiniteDuration]): SendMessageRequest = {
    val builder = SendMessageRequest.builder.messageBody(body).queueUrl(queueUrl.url)
    delayDuration.map(delay => builder.delaySeconds(delay.toMillis.toInt))
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

  private[sqs] def toMessageBatchEntry(batchId: String,
                                       delaySeconds: Option[FiniteDuration]): SendMessageBatchRequestEntry = {
    val builder = SendMessageBatchRequestEntry.builder
      .messageBody(body)
      .id(batchId)
    deduplicationId.map(builder.messageDeduplicationId)
    groupId.map(builder.messageGroupId)
    delaySeconds.map(delay => builder.delaySeconds(delay.toMillis.toInt))
    builder.messageAttributes(messageAttributes.map { case (k, v) => (k, v.toAttrValue) }.asJava)
    awsTraceHeader.map { attr =>
      builder.messageSystemAttributes(
        Map(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER -> attr.toSystemAttrValue).asJava
      )
    }
    builder.build
  }

}
