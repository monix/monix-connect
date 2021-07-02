package monix.connect.dynamodb

import monix.connect.aws.auth.MonixAwsConf
import monix.connect.dynamodb.domain.RetryStrategy
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.model.{GetItemRequest, GetItemResponse, ListTablesRequest, ListTablesResponse, PutItemRequest}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class DynamoDbSuite extends AnyFlatSpec with Matchers with DynamoDbFixture with BeforeAndAfterAll {

  import monix.connect.dynamodb.DynamoDbOp.Implicits._

  s"$DynamoDbOp" can "be created from config" in {
    //given
    import monix.connect.dynamodb.DynamoDbOp.Implicits.listTablesOp
    val listRequest = ListTablesRequest.builder.build

    //when
    val t: Task[ListTablesResponse] = DynamoDb.fromConfig.use(_.single(listRequest))

    //then
    val listedTables = t.runSyncUnsafe()
    listedTables shouldBe a[ListTablesResponse]
    listedTables.tableNames().asScala.contains(tableName) shouldBe true
  }

  it can "be created from monix aws config" in {
    //given
    import monix.connect.dynamodb.DynamoDbOp.Implicits.listTablesOp
    val listRequest = ListTablesRequest.builder.build

    //when
    val listTables: ListTablesResponse = MonixAwsConf.load().memoizeOnSuccess.flatMap(DynamoDb.fromConfig(_).use(_.single(listRequest))).runSyncUnsafe()

    //then
    listTables shouldBe a[ListTablesResponse]
    listTables.tableNames().asScala.contains(tableName) shouldBe true
  }

  it can "be created from task with monix aws config" in {
    //given
    import monix.connect.dynamodb.DynamoDbOp.Implicits.listTablesOp
    val listRequest = ListTablesRequest.builder.build

    //when
    val monixAwsConf = MonixAwsConf.load().memoizeOnSuccess
    val listTables: ListTablesResponse = DynamoDb.fromConfig(monixAwsConf).use(_.single(listRequest)).runSyncUnsafe()

    //then
    listTables shouldBe a[ListTablesResponse]
    listTables.tableNames().asScala.contains(tableName) shouldBe true
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
    import monix.connect.dynamodb.DynamoDbOp.Implicits.putItemOp
    val citizen = genCitizen.sample.get
    val request: PutItemRequest = putItemRequest(tableName, citizen)

    //when
    Observable.now(request).consumeWith(DynamoDb.createUnsafe(client).sink()).runSyncUnsafe()

    //then
    val getResponse = Task.from(client.getItem(getItemRequest(tableName, citizen.citizenId, citizen.city))).runSyncUnsafe()
    getResponse.item().values().asScala.head.n().toDouble shouldBe citizen.age
  }

  it can "sink multiple incoming requests" in {
    //given
    import monix.connect.dynamodb.DynamoDbOp.Implicits.putItemOp
    val citizens = Gen.listOfN(3, genCitizen).sample.get
    val putItemRequests: List[PutItemRequest] = citizens.map(putItemRequest(tableName, _))

    //when
    DynamoDb.fromConfig.use{ dynamoDb =>
      Observable
        .fromIterable(putItemRequests)
        .consumeWith(dynamoDb.sink(RetryStrategy(retries = 3, backoffDelay = 1.second)))
    }.runSyncUnsafe()

    //then
    val getResponses = Task.traverse(citizens)(citizen => Task.from(client.getItem(getItemRequest(tableName, citizen.citizenId, citizen.city)))).runSyncUnsafe()
    getResponses.map(_.item().values().asScala.head.n().toDouble) should contain theSameElementsAs citizens.map(_.age)
  }

  it can "transform multiple incoming requests" in {
    //given
    import monix.connect.dynamodb.DynamoDbOp.Implicits.getItemOp
    val citizens = List(Citizen("citizen1", "Rome", 52), Citizen("citizen2", "Rome", 43))
    val putItemRequests: List[PutItemRequest] = citizens.map(putItemRequest(tableName, _))
    Task.parSequence(putItemRequests.map(req => Task.from(client.putItem(req)))).runSyncUnsafe()

    //and
    val getItemRequests: List[GetItemRequest] = List("citizen1", "citizen2").map(getItemRequest(tableName, _, city = "Rome"))

    //then
    val f: List[GetItemResponse] = DynamoDb.fromConfig.use{ dynamoDb =>
      Observable
        .fromIterable(getItemRequests)
        .transform(dynamoDb.transformer(RetryStrategy(retries = 3, backoffDelay = 1.second)))
        .toListL
    }.runSyncUnsafe()

    //then
    f.map(_.item().values().asScala.head.n().toInt) should contain theSameElementsAs citizens.map(_.age)
  }

  override def beforeAll(): Unit = {
    createTable(tableName).runSyncUnsafe()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
