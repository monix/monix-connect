package monix.connect.dynamodb

import java.lang.Thread.sleep

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.JavaConverters._

class DynamoDbSuite extends AnyFlatSpec with Matchers with DynamoDbFixture with BeforeAndAfterAll {

  import monix.connect.dynamodb.DynamoDbOp.Implicits._

  s"${DynamoDbOp}" can "be created from config" in {
    //given
    val listRequest = ListTablesRequest.builder.build

    //when
    val t: Task[ListTablesResponse] = DynamoDb.fromConfig.use(_.single(listRequest))

    //then
    val listedTables = t.runSyncUnsafe()
    listedTables shouldBe a[ListTablesResponse]
    listedTables.tableNames().asScala.contains(tableName) shouldBe true
  }

  it can "be safely created from the parameter configurations" in {
    //given
    val listRequest = ListTablesRequest.builder.build

    //when
    val dynamodbResource = DynamoDb.create(staticAwsCredProvider, Region.AWS_GLOBAL, endpoint = Some("http://localhost:4569"))
    val t: Task[ListTablesResponse] = dynamodbResource.use(_.single(listRequest))

    //then
    val listedTables = t.runSyncUnsafe()
    listedTables shouldBe a[ListTablesResponse]
    listedTables.tableNames().asScala.contains(tableName) shouldBe true
  }

  it can "be created unsafely" in {
    //given
    val citizen = genCitizen.sample.get
    val request: PutItemRequest = putItemRequest(tableName, citizen)

    //when
    val t: Task[PutItemResponse] = DynamoDb.createUnsafe(client).single(request)

    //then
    t.runSyncUnsafe() shouldBe a[PutItemResponse]
    val getResponse = Task.from(client.getItem(getItemRequest(tableName, citizen.citizenId, citizen.city))).runSyncUnsafe()
    getResponse.item().values().asScala.head.n().toDouble shouldBe citizen.debt
  }

  override def beforeAll(): Unit = {
    createTable(tableName).runSyncUnsafe()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
