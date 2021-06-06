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

package monix.connect.sqs.producer

import monix.connect.sqs.domain.MessageAttribute

import scala.concurrent.duration.FiniteDuration

/**
  * The message type representation to be sent to a Standard Sqs queue.
  *
  * The main differences between [[FifoMessage]] is that it does not need
  * `deduplication` nor `groupId`, on the other hand it can contain `delayDuration`,
  * which is not supported in fifo queues.
  *
  * @see <a href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html">Standard queues.</a>.
  *
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
  * @param delayDuration The length of time, in seconds, for which a specific message is delayed.
  *                      Valid values: 0 to 900. Maximum: 15 minutes.
  *                      Messages with a positive DelaySeconds value become available
  *                      for processing after the delay period is finished.
  *                      If you don't specify a value, the default value for the queue is applied.
  *                      [[FifoMessage]]s do not support this field because it is not supported
  *                      by FifoQueues.
  *                      You can set this parameter only on a queue level.
  *
  */
final case class StandardMessage(
  body: String,
  messageAttributes: Map[String, MessageAttribute] = Map.empty,
  awsTraceHeader: Option[MessageAttribute] = None,
  delayDuration: Option[FiniteDuration] = None)
  extends Message(body, groupId = None, deduplicationId = None, messageAttributes, awsTraceHeader, delayDuration)
