package monix.connect.dynamodb


import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import monix.connect.dynamodb.DynamoDbOp.Implicits._
import org.scalacheck.Gen
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.model._

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._

class DynamoDbConsumerSuite
  extends AnyWordSpecLike with Matchers with ScalaFutures with DynamoDbFixture with BeforeAndAfterAll {

  implicit val defaultConfig: PatienceConfig = PatienceConfig(10.seconds, 300.milliseconds)

  s"${DynamoDb}.consumer() creates a ${Consumer}" that {

    s"given an implicit instance of ${DynamoDbOp.Implicits.createTableOp} in the scope" must {

      s"consume a single `CreateTableRequest` and materializes to `CreateTableResponse`" in {
        //given
        val randomTableName: String = genTableName.sample.get //a table must be between 3 and 255 characters
        val consumer: Consumer[CreateTableRequest, Unit] =
          DynamoDb.consumer[CreateTableRequest, CreateTableResponse]()
        val request =
          createTableRequest(tableName = randomTableName, schema = keySchema, attributeDefinition = tableDefinition)

        //when
        val t: Task[Unit] =
          Observable.pure(request).consumeWith(consumer)

        //then
        whenReady(t.runToFuture) { response: Unit =>
          response shouldBe a[Unit]
          response shouldBe ()
          Task.from(client.listTables()).runSyncUnsafe().tableNames().contains(randomTableName)
        }
      }
    }

    s"with an implicit instance of ${DynamoDbOp.Implicits.putItemOp} in the scope" must {

      s"consume a single `PutItemRequest`" in {
        //given
        val consumer: Consumer[PutItemRequest, Unit] =
          DynamoDb.consumer[PutItemRequest, PutItemResponse]()
        val city = Gen.identifier.sample.get
        val citizenId = Gen.identifier.sample.get
        val age = Gen.choose(0, 100).sample.get
        val request: PutItemRequest = putItemRequest(tableName, city, citizenId, age)

        //when
        val t: Task[Unit] = Observable.pure(request).consumeWith(consumer)

        //then
        whenReady(t.runToFuture) { r =>
          r shouldBe a[Unit]
          val getResponse: GetItemResponse = Task.from(client.getItem(getItemRequest(tableName, city, citizenId))).runSyncUnsafe()
          getResponse.item().values().asScala.head.n().toDouble shouldBe age
        }
      }

      s"consume multiple `PutItemRequests`" in {
        //given
        val consumer: Consumer[PutItemRequest, Unit] =
          DynamoDb.consumer[PutItemRequest, PutItemResponse]()
        val requestAttr: List[Citizen] = Gen.nonEmptyListOf(genCitizen).sample.get
        val requests: List[PutItemRequest] = requestAttr.map { citizen => putItemRequest(tableName, citizen.citizenId, citizen.city, citizen.age) }

        //when
        val t: Task[Unit] = Observable.fromIterable(requests).consumeWith(consumer)

        //then
        whenReady(t.runToFuture) { r =>
          r shouldBe a[Unit]
          requestAttr.map { citizen =>
            val getResponse: GetItemResponse = toScala(client.getItem(getItemRequest(tableName, citizen.citizenId, citizen.city))).futureValue
            getResponse.item().values().asScala.head.n().toDouble shouldBe citizen.age
          }
        }
      }
    }

    s"with an implicit instance of ${DynamoDbOp.Implicits.getItemOp} in the scope" must {

      //WARNING: Consuming an Observable of GetItemRequests it is not useful at all, but anyways it is something that can be done.
      s"consume a single `GetItemRequest` and materialize it to `GetItemResponse` " in {
        //given
        val city = "Barcelona"
        val citizenId = Gen.identifier.sample.get
        val age: Int = 1015
        Task.from(client.putItem(putItemRequest(tableName, city, citizenId, age))).runSyncUnsafe()
        val request: GetItemRequest = getItemRequest(tableName, city, citizenId)

        //when
        val t: Task[Unit] =
          Observable.pure(request).consumeWith(DynamoDb.consumer())

        //then
        whenReady(t.runToFuture) { response =>
          response shouldBe a[Unit]
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
