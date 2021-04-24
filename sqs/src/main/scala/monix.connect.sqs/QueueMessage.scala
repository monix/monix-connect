package monix.connect.sqs

import org.apache.commons.codec.digest.DigestUtils.{sha1Hex, sha256}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.sqs.model.{MessageAttributeValue, MessageSystemAttributeNameForSends, MessageSystemAttributeValue, SendMessageBatchRequestEntry, SendMessageRequest}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

case class QueueMessage(body: String,
                        deduplicationId: Option[String],
                        messageAttributes: Map[String, MessageAttribute] = Map.empty,
                        awsTraceHeader: Option[MessageAttribute]) {

  def deduplicatedId: String = deduplicationId.getOrElse(sha1Hex(body))

  def toMessageRequest[Attr](queueUrl: String,
                             groupId: Option[String],
                             delayDuration: Option[FiniteDuration]): SendMessageRequest = {
    val builder = SendMessageRequest.builder.messageBody(body).queueUrl(queueUrl)
    delayDuration.map(delay => builder.delaySeconds(delay.toMillis.toInt))
    groupId.map(builder.messageGroupId)
    builder.messageDeduplicationId(deduplicatedId)
    builder.messageAttributes(messageAttributes.map { case (k, v) => (k, v.toAttrValue) }.asJava)
    //todo test that MessageSystemAttributeNameForSend is only AWSTRACEHEADER
    awsTraceHeader.map { attr =>
      builder.messageSystemAttributes(
        Map(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER -> attr.toSystemAttrValue).asJava
      )
    }
    builder.build
  }

  def toMessageBatchEntry(batchId: String,
                          groupId: Option[String],
                          delaySeconds: Option[FiniteDuration]): SendMessageBatchRequestEntry = {
    val builder = SendMessageBatchRequestEntry.builder
      .messageBody(body)
      .messageDeduplicationId(deduplicatedId)
      .id(batchId)
    groupId.map(builder.messageGroupId)
    delaySeconds.map(delay => builder.delaySeconds(delay.toMillis.toInt))
    builder.messageAttributes(messageAttributes.map { case (k, v) => (k, v.toAttrValue) }.asJava)
    //todo test that MessageSystemAttributeNameForSend is only AWSTRACEHEADER
    awsTraceHeader.map { attr =>
      builder.messageSystemAttributes(
        Map(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER -> attr.toSystemAttrValue).asJava
      )
    }
    builder.build
  }
}
