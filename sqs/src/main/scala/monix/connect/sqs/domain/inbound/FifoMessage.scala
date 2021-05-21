package monix.connect.sqs.domain.inbound

import monix.connect.sqs.MessageAttribute

/**
  * The message representation to be sent to a FIFO queue.
  *
  * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">Fifo queues.</a>.
  *
  */
case class FifoMessage(body: String,
                       groupId: String,
                       deduplicationId: Option[String] = Option.empty,
                       messageAttributes: Map[String, MessageAttribute] = Map.empty,
                       awsTraceHeader: Option[MessageAttribute] = Option.empty)
  extends InboundMessage(body, groupId = Some(groupId), deduplicationId = deduplicationId, messageAttributes, awsTraceHeader)
