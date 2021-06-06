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

import monix.connect.sqs.domain.{BinaryMessageAttribute, QueueUrl, StringMessageAttribute}
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.sqs.model.{
  MessageAttributeValue,
  MessageSystemAttributeNameForSends,
  MessageSystemAttributeValue
}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class MessageSpec extends AnyFlatSpecLike with Matchers {

  val genId: Gen[String] = Gen.identifier.map(_.take(15))
  val genQueueUrl: Gen[QueueUrl] = QueueUrl(genId.sample.get)

  "A single fifo message" can "be converted to a java `MessageRequest`" in {
    //given
    val body = genId.sample.get
    val queueUrl = genQueueUrl.sample.get
    val deduplicationId = Gen.option(genId).sample.get
    val groupId = genId.sample.get
    val messageAttributes = Map("key" -> StringMessageAttribute("value"))
    val awsTraceHeader = Gen.option(genId).map(_.map(StringMessageAttribute)).sample.get

    val message = FifoMessage(body, groupId, deduplicationId = deduplicationId, messageAttributes, awsTraceHeader)

    //when
    val messageRequest = message.toMessageRequest(queueUrl)

    //then
    messageRequest.delaySeconds() shouldBe null
    messageRequest.messageGroupId() shouldBe groupId
    messageRequest.queueUrl() shouldBe queueUrl.url
    Option(messageRequest.messageDeduplicationId()) shouldBe message.deduplicationId
    messageRequest.messageAttributes().asScala.toMap.keys shouldBe messageAttributes.keys
    messageRequest.messageAttributes().asScala.toMap shouldBe messageAttributes.map {
      case (k, v) => (k, v.toAttrValue)
    }
    messageRequest.hasMessageSystemAttributes shouldBe awsTraceHeader.isDefined
    messageRequest.messageSystemAttributes().asScala.toMap shouldBe awsTraceHeader
      .map(header => Map(MessageSystemAttributeNameForSends.AWS_TRACE_HEADER -> header.toSystemAttrValue))
      .getOrElse(Map.empty)
  }

  "MessageSystemAttributeNameForSends" should "only be formed by awsTraceHeader" in {
    MessageSystemAttributeNameForSends.values().size shouldBe 2
    MessageSystemAttributeNameForSends.values() shouldBe Array(
      MessageSystemAttributeNameForSends.AWS_TRACE_HEADER,
      MessageSystemAttributeNameForSends.UNKNOWN_TO_SDK_VERSION)
  }

  "A StringMessageAttribute" can "be converted to java aws" in {
    val attributeValue = genId.sample.get
    val stringMessageAttr = StringMessageAttribute(attributeValue)

    stringMessageAttr.toSystemAttrValue shouldBe MessageSystemAttributeValue
      .builder()
      .stringValue(attributeValue)
      .dataType("String")
      .build()
    stringMessageAttr.toAttrValue shouldBe MessageAttributeValue
      .builder()
      .stringValue(attributeValue)
      .dataType("String")
      .build()
  }

  "A BinaryMessageAttribute" can "be converted to java aws" in {
    val attributeValue = genId.sample.get.getBytes
    val binaryMessageAttr = BinaryMessageAttribute(attributeValue)

    binaryMessageAttr.toSystemAttrValue shouldBe MessageSystemAttributeValue
      .builder()
      .binaryValue(SdkBytes.fromByteArray(attributeValue))
      .dataType("Binary")
      .build()
    binaryMessageAttr.toAttrValue shouldBe MessageAttributeValue
      .builder()
      .binaryValue(SdkBytes.fromByteArray(attributeValue))
      .dataType("Binary")
      .build()
  }
}
