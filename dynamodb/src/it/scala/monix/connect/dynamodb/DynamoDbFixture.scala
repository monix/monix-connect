package monix.connect.dynamodb

import monix.eval.Task
import monix.execution.Scheduler
import org.scalacheck.Gen
import org.scalatest.TestSuite
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, CreateTableRequest, DeleteTableRequest, DeleteTableResponse, GetItemRequest, KeySchemaElement, KeyType, ProvisionedThroughput, PutItemRequest, ScalarAttributeType}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait DynamoDbFixture {
  this: TestSuite =>

  case class Item(humanId: String, city: String, debt: Double) {
    val attributes = itemAttr(humanId, city, debt)
  }
  val strAttr: String => AttributeValue = value => AttributeValue.builder().s(value).build()
  val numAttr: Int => AttributeValue = value => AttributeValue.builder().n(value.toString).build()
  val doubleAttr: Double => AttributeValue = value => AttributeValue.builder().n(value.toString).build()

  val genTableName: Gen[String] =  Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "test-" + chars.mkString.take(200))  //table name max size is 255

  val genCitizenId = Gen.choose(1, 100000)
  val keyMap = (humanId: String, city: String) => Map("humanId" -> strAttr(humanId), "city" -> strAttr(city))

  val itemAttr = (humanId: String, city: String, debt: Double) => keyMap(humanId, city) ++ Map("debt" -> doubleAttr(debt))

  def putItemRequest(tableName: String, humanId: String, city: String, debt: Double): PutItemRequest =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(itemAttr(humanId, city, debt).asJava)
      .build()

  val genCity = Gen.oneOf(Seq("Barcelona", "London", "Istambul", "Milton Keynes", "Paris"))

  val genItem: Gen[Item] =
    for {
      humanId <- Gen.identifier
      city <- genCity
      debt <- Gen.choose(1, 1000)
    } yield Item(humanId, city, debt)

  val genPutItemRequest: Gen[PutItemRequest] =
    for {
      humanId <- Gen.identifier
      city <- genCity
      debt <- Gen.choose(1, 1000)
    } yield putItemRequest(tableName, humanId, city, debt)

  def genPutItemRequests: Gen[List[PutItemRequest]] = Gen.listOfN(3, genPutItemRequest)

  def getItemRequest(tableName: String, humanId: String, city: String) =
    GetItemRequest.builder().tableName(tableName).key(keyMap(humanId, city).asJava).attributesToGet("debt").build()

  val getItemMalformedRequest =
    GetItemRequest.builder().tableName(tableName).attributesToGet("not_present").build()

  protected val keySchema: List[KeySchemaElement] = {
    List(
      KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName("city").keyType(KeyType.RANGE).build()
    )
  }

  protected val tableDefinition: List[AttributeDefinition] = {
    List(
      AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
      AttributeDefinition.builder().attributeName("city").attributeType(ScalarAttributeType.S).build()
    )
  }

  protected val baseProvisionedThroughput =
    ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()

  val tableName = "cities_test"

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

  protected def createTable(table: String)(client: DynamoDbAsyncClient): Unit = {
    val request: CreateTableRequest =
      createTableRequest(tableName = table, schema = keySchema, attributeDefinition = tableDefinition)
    Try(Task.from(client.createTable(request))) match {
      case Success(_) => println(s"Table ${table} was created")
      case Failure(exception) => println("Failed to create table cities with exception: " + exception)
    }
  }

  def deleteTable(tableName: String)(implicit client: DynamoDbAsyncClient, scheduler: Scheduler): Task[DeleteTableResponse] = {
    val deleteRequest: DeleteTableRequest = DeleteTableRequest.builder().tableName(tableName).build()
    Task.from(client.deleteTable(deleteRequest))
  }

  def genRequestAttributes: Gen[(String, Int, Double)] = {
    for {
      city <- Gen.nonEmptyListOf(Gen.alphaChar)
      citizenId <- genCitizenId
      debt <- Gen.choose(0, 10000)
    } yield ("-" + city.mkString, citizenId, debt.toDouble) // '-' was added to avoid empty strings
  }

}
