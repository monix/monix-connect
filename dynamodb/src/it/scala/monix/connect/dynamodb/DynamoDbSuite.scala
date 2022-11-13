package monix.connect.dynamodb

import monix.connect.aws.auth.MonixAwsConf
import monix.connect.dynamodb.domain.RetryStrategy
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskTest
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, ListTablesRequest, ListTablesResponse, PutItemRequest}

import scala.concurrent.duration._

class DynamoDbSuite extends AsyncFlatSpec with Matchers with MonixTaskTest with DynamoDbFixture with BeforeAndAfterAll {

  override implicit val scheduler = Scheduler.io("dynamodb-suite")

  s"$DynamoDbOp" can "be created from config" in {
    import monix.connect.dynamodb.DynamoDbOp.Implicits.listTablesOp
    val listRequest = ListTablesRequest.builder.build

    DynamoDb.fromConfig().use(_.single(listRequest)).asserting { listedTables =>
      listedTables shouldBe a[ListTablesResponse]
      listedTables.tableNames().contains(tableName) shouldBe true
    }
  }

  it can "be created from monix aws config" in {
    import monix.connect.dynamodb.DynamoDbOp.Implicits.listTablesOp
    val listRequest = ListTablesRequest.builder.build

    MonixAwsConf.load().memoizeOnSuccess.flatMap(DynamoDb.fromConfig(_).use(_.single(listRequest))).asserting { listTables =>
      listTables shouldBe a[ListTablesResponse]
      listTables.tableNames().contains(tableName) shouldBe true
    }
  }

  it can "be created from task with monix aws config" in {
    import monix.connect.dynamodb.DynamoDbOp.Implicits.listTablesOp
    val listRequest = ListTablesRequest.builder.build

    val monixAwsConf = MonixAwsConf.load().memoizeOnSuccess
    DynamoDb.fromConfig(monixAwsConf).use(_.single(listRequest)).asserting { listTables =>
      listTables shouldBe a[ListTablesResponse]
      listTables.tableNames().contains(tableName) shouldBe true
    }
  }

  it can "be safely created from the parameter configurations" in {
    //given
    import monix.connect.dynamodb.DynamoDbOp.Implicits.listTablesOp
    val listRequest = ListTablesRequest.builder.build

    //when
    DynamoDb.create(staticAwsCredProvider, Region.AWS_GLOBAL, endpoint = Some("http://localhost:4569")).use { dynamodb =>
      Observable.now(listRequest).transform(dynamodb.transformer()).headL
    }.asserting { listedTables =>
      listedTables shouldBe a[ListTablesResponse]
      listedTables.tableNames().contains(tableName) shouldBe true
    }
  }

  it can "be created unsafely" in {
    import monix.connect.dynamodb.DynamoDbOp.Implicits.putItemOp
    val citizen = genCitizen.sample.get
    val request: PutItemRequest = putItemRequest(tableName, citizen)

    for {
      _ <- Observable.now(request).consumeWith(DynamoDb.createUnsafe(client).sink())
      getResponse <- Task.from(client.getItem(getItemRequest(tableName, citizen.citizenId, citizen.city)))
    } yield {
      getResponse.item().get(0).n().toDouble shouldBe citizen.age
    }
  }

  it can "sink multiple incoming requests" in {
    import monix.connect.dynamodb.DynamoDbOp.Implicits.putItemOp
    val citizens = Gen.listOfN(3, genCitizen).sample.get
    val putItemRequests: List[PutItemRequest] = citizens.map(putItemRequest(tableName, _))

    for {
      _ <- DynamoDb.fromConfig().use { dynamoDb =>
        Observable
          .fromIterable(putItemRequests)
          .consumeWith(dynamoDb.sink(RetryStrategy(retries = 3, backoffDelay = 1.second)))
      }
      getResponses <- Task.traverse(citizens) { citizen =>
        Task.from(client.getItem(getItemRequest(tableName, citizen.citizenId, citizen.city)))
      }
    } yield {
      val actualCitizens = getResponses.map(_.item().get(0).n().toDouble)
      actualCitizens should contain theSameElementsAs citizens.map(_.age)
    }
  }

  it can "transform multiple incoming requests" in {
    import monix.connect.dynamodb.DynamoDbOp.Implicits.getItemOp
    val citizens = List(Citizen("citizen1", "Rome", 52), Citizen("citizen2", "Rome", 43))
    val putItemRequests: List[PutItemRequest] = citizens.map(putItemRequest(tableName, _))
    val getItemRequests: List[GetItemRequest] = List("citizen1", "citizen2").map(getItemRequest(tableName, _, city = "Rome"))

    Task.traverse(putItemRequests)(req => Task.from(client.putItem(req))) >>
      DynamoDb.fromConfig().use { dynamoDb =>
        Observable
          .fromIterable(getItemRequests)
          .transform(dynamoDb.transformer(RetryStrategy(retries = 3, backoffDelay = 1.second)))
          .toListL
      }.map { result =>
        val actualCitizens = result.map(_.item().get(0).n().toInt)
        actualCitizens should contain theSameElementsAs citizens.map(_.age)
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
