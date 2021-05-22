package monix.connect.sqs.domain.inbound

import monix.connect.sqs.MessageAttribute

/**
  * The message representation to be sent to a FIFO queue.
  *
  * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html">Fifo queues.</a>.
  * @param body message content
  * @param messageAttributes structured metadata (such as timestamps, signatures, and identifiers)
  *                          that goes alongside the message. and that the consumer can use it
  *                          to handle a in a particular way without having to process the message body first.
  *                          Each message can have up to 10 attributes. See more in the
  *                          <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes">
  *                            SQS AWS Message Attributes gide.</a>.
  * @param awsTraceHeader  a message system attribute to carry the X-Ray trace header with messages in the queue.
  *                        Its type must be String and its value must be a correctly formatted AWS X-Ray trace header string.
  *                        The size of a message system attribute doesn't count towards the total size of a message.
  *                        See more in the <a href="https://docs.aws.amazon.com/xray/latest/devguide/xray-services-sqs.html">Xray sqs service docs.</a>.
  */
final case class FifoMessage(body: String,
                       groupId: String,
                       deduplicationId: Option[String] = Option.empty,
                       messageAttributes: Map[String, MessageAttribute] = Map.empty,
                       awsTraceHeader: Option[MessageAttribute] = Option.empty)
  extends InboundMessage(body, groupId = Some(groupId), deduplicationId = deduplicationId, messageAttributes, awsTraceHeader)
