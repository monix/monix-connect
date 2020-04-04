package scalona.monix.connectors.dynamodb

import java.util.concurrent.CompletableFuture

import org.scalacheck.Gen
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, CreateTableRequest, CreateTableResponse, DeleteTableRequest, KeySchemaElement, KeyType, ProvisionedThroughput, PutItemRequest, ScalarAttributeType}

import scala.jdk.CollectionConverters._

trait DynamoDbFixture {

  val strAttr: String => AttributeValue = value => AttributeValue.builder().s(value).build()
  val numAttr: Int => AttributeValue = value => AttributeValue.builder().n(value.toString).build()

  val pickCitizens = Gen.oneOf(1 to 100000)
  val item = (city: String, citizens: Int) =>  Map("city" -> strAttr(city), "population" -> numAttr(citizens))

  def genItem: Gen[Map[String, AttributeValue]] = Gen.oneOf(Seq(item("BCN", pickCitizens.sample.get)))

  def putItemRequest(tableName: String, mapAttr: Map[String, AttributeValue]): PutItemRequest =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(mapAttr.asJava)
      .build()

  def genPutItemRequest: Gen[PutItemRequest] = Gen.oneOf(Seq(putItemRequest(tableName,  item("Barcelonaaa", 10))))
  def genPutItemRequests = Gen.listOfN(10, genPutItemRequest)

  protected val keySchema: List[KeySchemaElement] = {
    List(
      KeySchemaElement.builder().attributeName("city").keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName("population").keyType(KeyType.RANGE).build()
    )
  }

  protected val tableDefinition: List[AttributeDefinition] = {
    List(
      AttributeDefinition.builder().attributeName("city").attributeType(ScalarAttributeType.S).build(),
      AttributeDefinition.builder().attributeName("population").attributeType(ScalarAttributeType.N).build()
    )
  }

  protected val baseProvisionedThroughput = ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()

  val tableName = "cities_test"

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
    val request: CreateTableRequest = createTableRequest(tableName = "cities", schema = keySchema, attributeDefinition = tableDefinition)
    client.createTable(request)
  }

  def deleteTable(tableName: String)(implicit client: DynamoDbAsyncClient): Unit = {
    val deleteRequest: DeleteTableRequest = DeleteTableRequest.builder().tableName(tableName).build()
    client.deleteTable(deleteRequest)
  }
}