package scalona.monix.connectors.dynamodb

import org.scalacheck.Gen
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, CreateTableRequest, DeleteTableRequest, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType}

trait DynamoDbFixture {

  protected val cityKeySchema: List[KeySchemaElement] = {
    List(
      KeySchemaElement.builder().attributeName("city").keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName("population").keyType(KeyType.RANGE).build()
    )
  }

  protected val cityAttrDef: List[AttributeDefinition] = {
    List(
      AttributeDefinition.builder().attributeName("city").attributeType(ScalarAttributeType.S).build(),
      AttributeDefinition.builder().attributeName("population").attributeType(ScalarAttributeType.N).build()
    )
  }

  protected val baseProvisionedThroughput = ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()

  val citiesTableName = "cities_test"

  def createTableRequest(tableName: String = Gen.alphaLowerStr.sample.get,
                         schema: List[KeySchemaElement],
                         attributeDefinition: List[AttributeDefinition],
                         provisionedThroughput: ProvisionedThroughput = baseProvisionedThroughput): CreateTableRequest = {
    CreateTableRequest
      .builder()
      .tableName(tableName)
      .keySchema(schema: _*)
      .attributeDefinitions(attributeDefinition: _*)
      .provisionedThroughput(provisionedThroughput)
      .build()
  }

  def deleteTable(tableName: String): Unit = {
    val deleteRequest: DeleteTableRequest = DeleteTableRequest.builder().tableName(citiesTableName).build()
    DynamoDbClient().deleteTable(deleteRequest)
  }
}