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

package monix.connect.sqs.domain

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.sqs.model.{MessageAttributeValue, MessageSystemAttributeValue}

sealed trait MessageAttribute {
  def toAttrValue: MessageAttributeValue
  def toSystemAttrValue: MessageSystemAttributeValue
}

final case class StringMessageAttribute(attributeValue: String) extends MessageAttribute {
  def toAttrValue: MessageAttributeValue = {
    MessageAttributeValue.builder.stringValue(attributeValue).dataType("String").build
  }

  def toSystemAttrValue: MessageSystemAttributeValue = {
    MessageSystemAttributeValue.builder
      .stringValue(attributeValue)
      .dataType("String")
      .build
  }
}

final case class BinaryMessageAttribute(attributeValue: Array[Byte]) extends MessageAttribute {

  def toAttrValue: MessageAttributeValue = {
    MessageAttributeValue.builder
      .binaryValue(SdkBytes.fromByteArray(attributeValue))
      .dataType("Binary")
      .build
  }

  def toSystemAttrValue: MessageSystemAttributeValue = {
    MessageSystemAttributeValue.builder
      .binaryValue(SdkBytes.fromByteArray(attributeValue))
      .dataType("Binary")
      .build
  }
}
