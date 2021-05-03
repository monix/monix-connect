package monix.connect.sqs.domain

import monix.connect.sqs.MessageAttribute

final case class StandardMessage(body: String,
                                messageAttributes: Map[String, MessageAttribute] = Map.empty,
                                awsTraceHeader: Option[MessageAttribute] = Option.empty)
  extends InboundMessage(body, groupId = None, deduplicationId = None, messageAttributes, awsTraceHeader)
