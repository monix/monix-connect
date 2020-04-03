package scalona.monix.connectors.dynamodb

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.concurrent.ScalaFutures._
import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import scalona.monix.connectors.common.Implicits._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class DynamoDbConsumerSpec extends AnyWordSpecLike with Matchers with ScalaFutures with DynamoDbFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 100.milliseconds)
  implicit val client: DynamoDbAsyncClient = DynamoDbClient()

  s"${DynamoDb}.transformer() " should {

    s"create a reactive Transformer" that {

      s"receives `CreateTableRequests` and transforms them to `CreateTableResponses`" in {

        //given
        val consumer: Consumer[CreateTableRequest, Task[CreateTableResponse]] =
          DynamoDb.consumer[CreateTableRequest, CreateTableResponse]
        val request = createTableRequest(tableName = citiesTableName, schema = cityKeySchema, attributeDefinition = cityAttrDef)

        //when
        val t: Task[CreateTableResponse] =
          Observable.fromIterable(Iterable(request)).consumeWith(consumer).runSyncUnsafe()

        //then
        whenReady(t.runToFuture) { response: CreateTableResponse =>
          response shouldBe a[CreateTableResponse]
          response.tableDescription().hasKeySchema shouldBe true
          response.tableDescription().hasAttributeDefinitions shouldBe true
          response.tableDescription().hasGlobalSecondaryIndexes shouldBe false
          response.tableDescription().hasReplicas shouldBe false
          response.tableDescription().tableName() shouldEqual citiesTableName
          response.tableDescription().keySchema() should contain theSameElementsAs cityKeySchema
          response.tableDescription().attributeDefinitions() should contain theSameElementsAs cityAttrDef
        }
      }

      s"receives `PutItemRequest` and returns `PutItemResponse` " in {

        //given
        createCitiesTable()
        val consumer: Consumer[PutItemRequest, Task[PutItemResponse]] =
          DynamoDb.consumer[PutItemRequest, PutItemResponse]
        val request: PutItemRequest = putItemRequest(citiesTableName, citiesMappAttr)

        //when
        val t: Task[PutItemResponse] =
          Observable.fromIterable(Iterable(request)).consumeWith(consumer).runSyncUnsafe()

        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[PutItemResponse]
          response.attributes().asScala should contain theSameElementsAs request.item().asScala
        }
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    deleteTable(citiesTableName)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
