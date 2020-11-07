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

  case class Item(employeeId: String, companyId: Int, salary: Double) {
    val attributes = itemAttr(employeeId, companyId, salary)
  }
  val strAttr: String => AttributeValue = value => AttributeValue.builder().s(value).build()
  val numAttr: Int => AttributeValue = value => AttributeValue.builder().n(value.toString).build()
  val doubleAttr: Double => AttributeValue = value => AttributeValue.builder().n(value.toString).build()

  val genTableName: Gen[String] =  Gen.nonEmptyListOf(Gen.alphaChar).map(chars => "test-" + chars.mkString.take(200))  //table name max size is 255

  val genCitizenId = Gen.choose(1, 100000)
  val keyMap = (employeeId: String, companyId: Int) => Map("employeeId" -> strAttr(employeeId), "companyId" -> numAttr(companyId))

  val itemAttr = (employeeId: String, companyId: Int, salary: Double) => keyMap(employeeId, companyId) ++ Map("salary" -> doubleAttr(salary))

  def putItemRequest(tableName: String, employeeId: String, companyId: Int, salary: Double): PutItemRequest =
    PutItemRequest
      .builder()
      .tableName(tableName)
      .item(itemAttr(employeeId, companyId, salary).asJava)
      .build()

  val genItem: Gen[Item] =
    for {
      employeeId <- Gen.identifier
      companyId <- Gen.choose(1, 1000)
      salary <- Gen.choose(1, 1000)
    } yield Item(employeeId, companyId, salary)

  val genPutItemRequest: Gen[PutItemRequest] =
    for {
      employeeId <- Gen.identifier
      companyId <- Gen.choose(1, 1000)
      salary <- Gen.choose(1, 1000)
    } yield putItemRequest(tableName, employeeId, companyId, salary)

  def genPutItemRequests: Gen[List[PutItemRequest]] = Gen.listOfN(3, genPutItemRequest)

  def getItemRequest(tableName: String, employeeId: String, companyId: Int) =
    GetItemRequest.builder().tableName(tableName).key(keyMap(employeeId, companyId).asJava).attributesToGet("salary").build()

  val getItemMalformedRequest =
    GetItemRequest.builder().tableName(tableName).attributesToGet("not_present").build()

  protected val keySchema: List[KeySchemaElement] = {
    List(
      KeySchemaElement.builder().attributeName("employeeId").keyType(KeyType.HASH).build(),
      KeySchemaElement.builder().attributeName("companyId").keyType(KeyType.RANGE).build()
    )
  }

  protected val tableDefinition: List[AttributeDefinition] = {
    List(
      AttributeDefinition.builder().attributeName("employeeId").attributeType(ScalarAttributeType.S).build(),
      AttributeDefinition.builder().attributeName("companyId").attributeType(ScalarAttributeType.N).build()
    )
  }

  protected val baseProvisionedThroughput =
    ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build()

  val tableName = "employeesepa"

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
      companyId <- Gen.nonEmptyListOf(Gen.alphaChar)
      citizenId <- genCitizenId
      salary <- Gen.choose(0, 10000)
    } yield ("-" + companyId.mkString, citizenId, salary.toDouble) // '-' was added to avoid empty strings
  }

}
