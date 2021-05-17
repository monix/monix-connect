package monix.connect.sqs.domain

import monix.connect.sqs.MessageAttribute

/**
  * The message representation to be sent to a Standard Sqs queue.
  *
  * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html">Standard queues.</a>.
  *
  */
final case class StandardMessage(body: String,
                                messageAttributes: Map[String, MessageAttribute] = Map.empty,
                                awsTraceHeader: Option[MessageAttribute] = Option.empty)
  extends InboundMessage(body, groupId = None, deduplicationId = None, messageAttributes, awsTraceHeader)
