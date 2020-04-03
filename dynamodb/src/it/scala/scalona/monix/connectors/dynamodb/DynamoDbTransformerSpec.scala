package scalona.monix.connectors.dynamodb

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import scalona.monix.connectors.common.Implicits.Transformer
import scalona.monix.connectors.common.Implicits._

import scala.jdk.CollectionConverters._

class DynamoDbTransformerSpec
  extends AnyWordSpecLike with Matchers with ScalaFutures with DynamoDbFixture with BeforeAndAfterAll {

  implicit val client: DynamoDbAsyncClient = DynamoDbClient()
  s"${DynamoDb}.transformer() " should {

    s"create a reactive `Transformer`" that {

      s"receives `CreateTableRequests` and returns `CreateTableResponses`" in {
        //given
        val transformer: Transformer[CreateTableRequest, Task[CreateTableResponse]] =
          DynamoDb.transformer[CreateTableRequest, CreateTableResponse]
        val request =
          createTableRequest(tableName = citiesTableName, schema = cityKeySchema, attributeDefinition = cityAttrDef)

        //when
        val ob: Observable[Task[CreateTableResponse]] =
          Observable
            .fromIterable(Iterable(request))
            .transform(transformer)
        val t: Task[CreateTableResponse] = ob.headL.runToFuture.futureValue

        //then
        whenReady(t.runToFuture) { response =>
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
    }

    s"receives `PutItemRequest` and returns `PutItemResponse` " in {

      //given
      createCitiesTable()
      val transformer: Transformer[PutItemRequest, Task[PutItemResponse]] =
        DynamoDb.transformer[PutItemRequest, PutItemResponse]
      val request: PutItemRequest = putItemRequest(citiesTableName, citiesMappAttr)

      //when
      val t: Task[PutItemResponse] =
        Observable.fromIterable(Iterable(request)).transform(transformer).headL.runToFuture.futureValue

      //then
      whenReady(t.runToFuture) { response =>
        response shouldBe a[PutItemResponse]
        response.attributes().asScala should contain theSameElementsAs request.item().asScala
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
