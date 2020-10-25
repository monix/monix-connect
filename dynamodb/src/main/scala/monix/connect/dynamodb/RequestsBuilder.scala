package monix.connect.dynamodb

import software.amazon.awssdk.services.dynamodb.model.GetItemRequest

import scala.jdk.CollectionConverters._

object RequestsBuilder {

  def getItemRequest(
    tableName: String,
    projectionExpression: Option[String],
    consistentRead: Boolean,
    expressionAttributeNames: Map[String, String],
    consumedCapacityDetails: ConsumedCapacity.Detail) = {
    val builder = GetItemRequest.builder().tableName(tableName)
      .consistentRead(consistentRead)
      .expressionAttributeNames(expressionAttributeNames.asJava)
      .returnConsumedCapacity(consumedCapacityDetails.toString)
    projectionExpression.map(builder.projectionExpression)
    builder.build()
  }

}
