package monix.connect.sqs.domain

import monix.connect.sqs.MessageAttribute
import software.amazon.awssdk.services.sqs.model.{MessageSystemAttributeNameForSends, SendMessageBatchRequestEntry, SendMessageRequest}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

class StandardMessage private[sqs](body: String,
                                   deduplicationId: Option[String] = Option.empty,
                                   messageAttributes: Map[String, MessageAttribute] = Map.empty,
                                   awsTraceHeader: Option[MessageAttribute] = Option.empty) {

  private[sqs] def toMessageRequest[Attr](queueUrl: QueueUrl,
                             groupId: Option[String],
                             delayDuration: Option[FiniteDuration]): SendMessageRequest = {
    val builder = SendMessageRequest.builder.messageBody(body).queueUrl(queueUrl.url)
    delayDuration.map(delay => builder.delaySeconds(delay.toMillis.toInt))
    groupId.map(builder.messageGroupId)
    deduplicationId.map(builder.messageDeduplicationId)
    builder.messageAttributes(messageAttributes.map { case (k, v) => (k, v.toAttrValue) }.asJava)
    //todo test that MessageSystemAttributeNameForSend is only AWSTRACEHEADER
    awsTraceHeader.map { attr =>
      builder.messageSystemAttributes(
        Map(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER -> attr.toSystemAttrValue).asJava
      )
    }
    builder.build
  }

  private[sqs] def toMessageBatchEntry(batchId: String,
                          groupId: Option[String],
                          delaySeconds: Option[FiniteDuration]): SendMessageBatchRequestEntry = {
    val builder = SendMessageBatchRequestEntry.builder
      .messageBody(body)
      .id(batchId)

    deduplicationId.map(builder.messageDeduplicationId)
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
}          delaySeconds: Option[FiniteDuration]): SendMessageBatchRequestEntry = {
                                       val builder = SendMessageBatchRequestEntry.builder
                                         .messageBody(body)
                                         .id(batchId)

                                       deduplicationId.map(builder.messageDeduplicationId)
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
