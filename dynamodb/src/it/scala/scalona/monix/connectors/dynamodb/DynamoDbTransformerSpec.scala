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
import scala.concurrent.duration._
import scala.jdk.FutureConverters._

class DynamoDbTransformerSpec
  extends AnyWordSpecLike with Matchers with ScalaFutures with DynamoDbFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 100.milliseconds)
  implicit val client: DynamoDbAsyncClient = DynamoDbClient()
  s"${DynamoDb}.transformer() " should {

    s"create a reactive `Transformer`" that {

      s"receives `CreateTableRequests` and transforms to `CreateTableResponses`" in {
        //given
        val transformer: Transformer[CreateTableRequest, Task[CreateTableResponse]] =
          DynamoDb.transformer[CreateTableRequest, CreateTableResponse]
        val request =
          createTableRequest(tableName = tableName, schema = keySchema, attributeDefinition = tableDefinition)

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
          response.tableDescription().tableName() shouldEqual tableName
          response.tableDescription().keySchema() should contain theSameElementsAs keySchema
          response.tableDescription().attributeDefinitions() should contain theSameElementsAs tableDefinition
        }
      }
    }

    s"receives a single`PutItemRequest` and transforms to `PutItemResponse` " in {
      //given
      createCitiesTable()
      val transformer: Transformer[PutItemRequest, Task[PutItemResponse]] =
        DynamoDb.transformer[PutItemRequest, PutItemResponse]
      val request: PutItemRequest = genPutItemRequest.sample.get

      //when
      val t: Task[PutItemResponse] =
        Observable.fromIterable(Iterable(request)).transform(transformer).headL.runToFuture.futureValue

      //then
      whenReady(t.runToFuture) { response =>
        response shouldBe a[PutItemResponse]
        response.attributes().asScala should contain theSameElementsAs request.item().asScala
      }
    }

    s"receives multiple `PutItemRequests` and transforms to `PutItemResponses` " in {
      //given
      val transformer: Transformer[PutItemRequest, Task[PutItemResponse]] =
        DynamoDb.transformer[PutItemRequest, PutItemResponse]
      val requests: List[PutItemRequest] = genPutItemRequests.sample.get

      //when
      val responses: List[Task[PutItemResponse]] =
        Observable.fromIterable(requests).transform(transformer).toListL.runToFuture.futureValue

      //then
      requests.zip(responses).foreach { case (req: PutItemRequest, f: Task[PutItemResponse]) =>
        whenReady(f.runToFuture) { response =>
          response shouldBe a[PutItemResponse]
          response.attributes().asScala should contain theSameElementsAs req.item().asScala
        }
      }
    }

    s"consumes a single `GetItemRequest` and transforms to `GetItemResponse` " in {
      //given
      val city = "London"
      val citizenId = 613371
      val debt: Int = 550
      client.putItem(putItemRequest(tableName, city, citizenId, debt)).asScala.futureValue
      val request: GetItemRequest = getItemRequest(tableName, city, citizenId)
      val transformer: Transformer[GetItemRequest, Task[GetItemResponse]] = DynamoDb.transformer

      //when
      val t: Task[GetItemResponse] =
        Observable.fromIterable(Iterable(request)).transform(transformer).headL.runToFuture.futureValue

      //then
      whenReady(t.runToFuture) { response =>
        response shouldBe a[GetItemResponse]
        response.hasItem shouldBe true
        response.item() should contain key "debt"
        response.item().values().asScala.head.n().toDouble shouldBe debt
      }
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    deleteTable(tableName)
    createCitiesTable()
  }

  override def afterAll(): Unit = {
    createCitiesTable()
    super.afterAll()
  }
}
