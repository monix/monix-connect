package monix.connect.sqs.domain

import monix.connect.sqs.MessageAttribute
import software.amazon.awssdk.services.sqs.model.{MessageSystemAttributeNameForSends, SendMessageBatchRequestEntry, SendMessageRequest}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

final case class FifoMessage(body: String,
                             groupId: String,
                             deduplicationId: Option[String] = Option.empty,
                             messageAttributes: Map[String, MessageAttribute] = Map.empty,
                             awsTraceHeader: Option[MessageAttribute] = Option.empty)
  extends InboundMessage(body, groupId = Some(groupId), deduplicationId = deduplicationId, messageAttributes, awsTraceHeader) {

}


