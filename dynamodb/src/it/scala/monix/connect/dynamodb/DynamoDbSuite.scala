package monix.connect.dynamodb

import java.lang.Thread.sleep

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
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
    val t: Task[ListTablesResponse] = dynamodbResource.use { dynamodb =>
      Observable.now(listRequest).transform(dynamodb.transformer()).headL
    }

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
    Observable.now(request).consumeWith(DynamoDb.createUnsafe(client).sink()).runSyncUnsafe()

    //then
    val getResponse = Task.from(client.getItem(getItemRequest(tableName, citizen.citizenId, citizen.city))).runSyncUnsafe()
    getResponse.item().values().asScala.head.n().toDouble shouldBe citizen.debt
  }

  "A real example" should "reuse the resource evaluation" in {
    import software.amazon.awssdk.services.dynamodb.model.{PutItemRequest, AttributeValue}

    //given
    val citizen1 = Citizen("001", "Lisbon", 100)
    val citizen2 = Citizen("001", "Lisbon", 100)
    val citizen3 = Citizen("001", "Lisbon", 100)

    val bucket = "my-bucket"
    val key = "my-key"
    val content = "my-content"
    val strAttr: String => AttributeValue = value => AttributeValue.builder.s(value).build

    def putItemRequest(tableName: String, citizenId: String, city: String, debt: Double): PutItemRequest =
      PutItemRequest
        .builder
        .tableName(tableName)
        .item(Map("citizenId" -> strAttr(citizenId), "city" -> strAttr(city)).asJava)
        .build

    def runDynamoDbApp(dynamoDb: DynamoDb): Task[Array[Byte]] = {
      for {
        _ <- s3.createBucket(bucket)
        _ <- s3.upload(bucket, key, content.getBytes)
        existsObject <- s3.existsObject(bucket, key)
        download <- {
          if(existsObject) s3.download(bucket, key)
          else Task.raiseError(NoSuchKeyException.builder().build())
        }
      } yield download
    }
  }

  override def beforeAll(): Unit = {
    createTable(tableName).runSyncUnsafe()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
