package scalona.monix.connectors.dynamodb

import java.util.concurrent.CompletableFuture

import org.scalacheck.Gen
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, CreateTableRequest, CreateTableResponse, DeleteTableRequest, GetItemRequest, KeySchemaElement, KeyType, ProvisionedThroughput, PutItemRequest, ScalarAttributeType}

import scala.jdk.CollectionConverters._

trait DynamoDbFixture {

  val strAttr: String => AttributeValue = value => AttributeValue.builder().s(value).build()
  val numAttr: Int => AttributeValue = value => AttributeValue.builder().n(value.toString).build()
  val doubleAttr: Double => AttributeValue = value => AttributeValue.builder().n(value.toString).build()

  val genCitizenId = Gen.oneOf(1 to 100000)
  val keyMap = (city: String, citizens: Int) =>  Map("city" -> strAttr(city), "citizenId" -> numAttr(citizens))

  val item = (city: String, citizens: Int, debt: Double) =>  keyMap(city, citizens) ++ Map("debt" -> doubleAttr(debt))

  def putItemRequest(tableName: String, city: String, citizens: Int, debt: Double): PutItemRequest =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(item(city, citizens, debt).asJava)
      .build()

  def genPutItemRequest: Gen[PutItemRequest] = Gen.oneOf(Seq(putItemRequest(tableName, Gen.alphaLowerStr.sample.get, genCitizenId.sample.get, genCitizenId.sample.get)))
  def genPutItemRequests = Gen.listOfN(10, genPutItemRequest)

  /*def getItemRequest(tableName: String, city: String, citizens: Int) =
    GetItemRequest.builder().tableName(tableName).key(keyMap("A", 0).asJava).attributesToGet("data").build()

  val getItemMalformedRequest =
    GetItemRequest.builder().tableName(tableName).attributesToGet("data").build()
*/
  protected val keySchema: List[KeySchemaElement] = {
    List(
      KeySchemaElement.builder().attributeName("city").keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName("citizenId").keyType(KeyType.RANGE).build()
    )
  }

  protected val tableDefinition: List[AttributeDefinition] = {
    List(
      AttributeDefinition.builder().attributeName("city").attributeType(ScalarAttributeType.S).build(),
      AttributeDefinition.builder().attributeName("citizenId").attributeType(ScalarAttributeType.N).build()
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