package monix.connect.sqs

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.sqs.model.{MessageAttributeValue, MessageSystemAttributeValue}

sealed trait MessageAttribute {
  def toAttrValue: MessageAttributeValue
  def toSystemAttrValue: MessageSystemAttributeValue
}

case class StringMessageAttribute(attributeValue: String) extends MessageAttribute {
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

class BinaryMessageAttribute(attributeValue: Array[Byte]) extends MessageAttribute {

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
