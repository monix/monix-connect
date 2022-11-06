package monix.connect.dynamodb

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.model._
import DynamoDbOp.Implicits._
import monix.reactive.Observable.Transformer

import scala.concurrent.duration._
import scala.collection.JavaConverters._

@deprecated("0.5.0")
class DynamoDbTransformerSuite
  extends AnyWordSpecLike with Matchers with ScalaFutures with DynamoDbFixture with BeforeAndAfterAll with Eventually {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)

  s"$DynamoDb.transformer() creates a transformer operator" that {

    s"given an implicit instance of ${DynamoDbOp.Implicits.createTableOp} in the scope" must {

      s"transform `CreateTableRequests` to `CreateTableResponses`" in {
        //given
        val randomTableName: String = genTableName.sample.get
        val transformer: Transformer[CreateTableRequest, CreateTableResponse] =
          DynamoDb.transformer[CreateTableRequest, CreateTableResponse]()
        val request =
          createTableRequest(tableName = randomTableName, schema = keySchema, attributeDefinition = tableDefinition)

        //when
        val ob: Observable[CreateTableResponse] =
          Observable
            .pure(request)
            .transform(transformer)
        val t: Task[CreateTableResponse] = ob.headL

        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[CreateTableResponse]
          response.tableDescription().hasKeySchema shouldBe true
          response.tableDescription().hasAttributeDefinitions shouldBe true
          response.tableDescription().hasGlobalSecondaryIndexes shouldBe false
          response.tableDescription().hasReplicas shouldBe false
          response.tableDescription().tableName() shouldEqual randomTableName
          response.tableDescription().keySchema() should contain theSameElementsAs keySchema
          response.tableDescription().attributeDefinitions() should contain theSameElementsAs tableDefinition
        }
      }
    }
    s"given an implicit instance of ${DynamoDbOp.Implicits.putItemOp} in the scope" must {

      s"transform a single`PutItemRequest` to `PutItemResponse` " in {
        //given
        val transformer: Transformer[PutItemRequest, PutItemResponse] =
          DynamoDb.transformer[PutItemRequest, PutItemResponse]()
        val city = Gen.identifier.sample.get
        val citizenId = Gen.identifier.sample.get
        val debt = Gen.choose(0, 10000).sample.get
        val request: PutItemRequest = putItemRequest(tableName, citizenId, city, debt)

        //when
        val t: Task[PutItemResponse] = Observable.fromIterable(Iterable(request)).transform(transformer).lastL

        //then
        whenReady(t.runToFuture) { r =>
          r shouldBe a[PutItemResponse]
          val getResponse: GetItemResponse =
            Task.from(client.getItem(getItemRequest(tableName, citizenId, city))).runSyncUnsafe()
          getResponse.item().values().asScala.head.n().toDouble shouldBe debt
        }
      }

      s"transform `PutItemRequests` to `PutItemResponses` " in {
        //given
        val transformer: Transformer[PutItemRequest, PutItemResponse] =
          DynamoDb.transformer[PutItemRequest, PutItemResponse]()
        val requestAttr: List[Citizen] = Gen.nonEmptyListOf(genCitizen).sample.get
        val requests: List[PutItemRequest] = requestAttr.map { citizen =>
          putItemRequest(tableName, citizen.citizenId, citizen.city, citizen.age)
        }

        //when
        val r =
          Observable
            .fromIterable(requests)
            .transform(transformer)
            .lastL
            .runSyncUnsafe()

        //then
        r shouldBe a[PutItemResponse]
        eventually {
          requestAttr.map { citizen =>
            val getResponse =
              Task.from(client.getItem(getItemRequest(tableName, citizen.citizenId, citizen.city))).runSyncUnsafe()
            getResponse.item().values().asScala.head.n().toDouble shouldBe citizen.age
          }
        }
      }
    }

    s"given an implicit instance of ${DynamoDbOp.Implicits.getItemOp} in the scope" must {

      s"transforms a single `GetItemRequest` to `GetItemResponse` " in {
        //given
        val city = "London"
        val citizenId = Gen.identifier.sample.get
        val age: Int = 34
        Task.from(client.putItem(putItemRequest(tableName, citizenId, city, age))).runSyncUnsafe()
        val request: GetItemRequest = getItemRequest(tableName, citizenId, city)
        val transformer: Transformer[GetItemRequest, GetItemResponse] = DynamoDb.transformer()

        //when
        val t: Task[GetItemResponse] =
          Observable.fromIterable(Iterable(request)).transform(transformer).headL

        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[GetItemResponse]
          response.hasItem shouldBe true
          response.item() should contain key "age"
          response.item().values().asScala.head.n().toDouble shouldBe age
        }
      }
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
