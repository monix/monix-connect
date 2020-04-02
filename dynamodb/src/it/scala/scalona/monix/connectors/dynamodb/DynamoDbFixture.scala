package scalona.monix.connectors.dynamodb

import java.util.concurrent.CompletableFuture

import org.scalacheck.Gen
import software.amazon.awssdk.services.dynamodb.{DynamoDbAsyncClient, DynamoDbClient}
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, CreateTableRequest, CreateTableResponse, DeleteTableRequest, KeySchemaElement, KeyType, ProvisionedThroughput, PutItemRequest, ScalarAttributeType}
import scala.collection.JavaConverters._
trait DynamoDbFixture {


  val strAttr: String => AttributeValue = value => AttributeValue.builder().s(value).build()
  val numAttr: Int => AttributeValue = value => AttributeValue.builder().n(value.toString).build()

  val citiesMappAttr: Map[String, AttributeValue] = Map("city" -> strAttr("Barcelona"), "population" -> numAttr(10000000))

  def putItemRequest(tableName: String, mapAttr: Map[String, AttributeValue]) =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(mapAttr.asJava)
      .build()

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

  protected def createCitiesTable()(implicit client: DynamoDbAsyncClient): CompletableFuture[CreateTableResponse] = {
    val request: CreateTableRequest = createTableRequest(tableName = "cities", schema = cityKeySchema, attributeDefinition = cityAttrDef)
    client.createTable(request)
  }

  def deleteTable(tableName: String)(implicit client: DynamoDbAsyncClient): Unit = {
    val deleteRequest: DeleteTableRequest = DeleteTableRequest.builder().tableName(citiesTableName).build()
    client.deleteTable(deleteRequest)
  }
}