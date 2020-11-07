package monix.connect.dynamodb

import java.net.URI

import monix.eval.Task
import monix.execution.Scheduler
import org.scalacheck.Gen
import org.scalatest.TestSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, CreateTableRequest, CreateTableResponse, DeleteTableRequest, DeleteTableResponse, GetItemRequest, KeySchemaElement, KeyType, ProvisionedThroughput, PutItemRequest, ScalarAttributeType}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

trait DynamoDbFixture {
  this: TestSuite =>

  case class Citizen(citizenId: String, city: String, debt: Double)
  val strAttr: String => AttributeValue = value => AttributeValue.builder().s(value).build()
  val numAttr: Int => AttributeValue = value => AttributeValue.builder().n(value.toString).build()
  val doubleAttr: Double => AttributeValue = value => AttributeValue.builder().n(value.toString).build()

  val staticAwsCredProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))

  val tableName = "citizens"

  protected implicit val client = DynamoDbAsyncClient
    .builder()
    .credentialsProvider(staticAwsCredProvider)
    .endpointOverride(new URI("http://localhost:4569"))
    .region(Region.AWS_GLOBAL)
    .build()

  val genTableName: Gen[String] =  Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "test-" + chars.mkString.take(200))  //table name max size is 255

  val genCitizenId = Gen.choose(1, 100000)
  val keyMap = (citizenId: String, city: String) => Map("citizenId" -> strAttr(citizenId), "city" -> strAttr(city))

  val item = (citizenId: String, city: String, debt: Double) => keyMap(citizenId, city) ++ Map("debt" -> doubleAttr(debt))

  def putItemRequest(tableName: String, citizen: Citizen): PutItemRequest = putItemRequest(tableName, citizen.citizenId, citizen.city, citizen.debt)

  def putItemRequest(tableName: String, citizenId: String, city: String, debt: Double): PutItemRequest =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(item(citizenId, city, debt).asJava)
      .build()

  val genPutItemRequest: Gen[PutItemRequest] =
    for {
      city <- Gen.identifier
      citizenId <- Gen.identifier
      debt <- Gen.choose(1, 1000)
    } yield putItemRequest(tableName, city, citizenId, debt)

  def genPutItemRequests: Gen[List[PutItemRequest]] = Gen.listOfN(3, genPutItemRequest)

  def getItemRequest(tableName: String, citizenId: String, city: String) =
    GetItemRequest.builder().tableName(tableName).key(keyMap(citizenId, city).asJava).attributesToGet("debt").build()

  val getItemMalformedRequest =
    GetItemRequest.builder().tableName(tableName).attributesToGet("not_present").build()

  protected val keySchema: List[KeySchemaElement] = {
    List(
      KeySchemaElement.builder().attributeName("citizenId").keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName("city").keyType(KeyType.RANGE).build()
    )
  }

  protected val tableDefinition: List[AttributeDefinition] = {
    List(
      AttributeDefinition.builder().attributeName("citizenId").attributeType(ScalarAttributeType.S).build(),
      AttributeDefinition.builder().attributeName("city").attributeType(ScalarAttributeType.S).build()
    )
  }

  protected val baseProvisionedThroughput =
    ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()

  def createTableRequest(
    tableName: String = Gen.alphaLowerStr.sample.get,
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

  protected def createTable(table: String)(implicit client: DynamoDbAsyncClient, scheduler: Scheduler): Task[CreateTableResponse] = {
    val request: CreateTableRequest =
      createTableRequest(tableName = table, schema = keySchema, attributeDefinition = tableDefinition)
    Task.from(client.createTable(request)).delayResult(1.second)
  }

  def deleteTable(tableName: String)(implicit client: DynamoDbAsyncClient, scheduler: Scheduler): Task[DeleteTableResponse] = {
    val deleteRequest: DeleteTableRequest = DeleteTableRequest.builder().tableName(tableName).build()
    Task.from(client.deleteTable(deleteRequest))
  }

  def genCitizen: Gen[Citizen] = {
    for {
      citizenId <- Gen.identifier
      city <- Gen.identifier
      debt <- Gen.choose(0, 10000)
    } yield Citizen(citizenId, city, debt.toDouble)
  }

}
