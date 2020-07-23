package monix.connect.dynamodb

import java.lang.Thread.sleep
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._

class DynamoDbOpSuite
  extends AnyWordSpecLike with Matchers with DynamoDbFixture with BeforeAndAfterAll {

  implicit val client: DynamoDbAsyncClient = DynamoDbClient()

  s"${DynamoDbOp} exposes a create method" that {

    import monix.connect.dynamodb.DynamoDbOp.Implicits._

    "defines the execution of any DynamoDb request" in {
      //given
      val city = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val citizenId = genCitizenId.sample.get
      val debt = Gen.choose(0, 10000).sample.get
      val request: PutItemRequest = putItemRequest(tableName, city, citizenId, debt)

      //when
      val t: Task[PutItemResponse] = DynamoDbOp.create(request)

      //then
      t.runSyncUnsafe() shouldBe a[PutItemResponse]
      val getResponse: GetItemResponse = Task.from(toScala(client.getItem(getItemRequest(tableName, city, citizenId)))).runSyncUnsafe()
      getResponse.item().values().asScala.head.n().toDouble shouldBe debt
    }

  }

  override def beforeAll(): Unit = {
    createTable(tableName)
    sleep(3000)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    deleteTable(tableName)
    super.afterAll()
  }
}
