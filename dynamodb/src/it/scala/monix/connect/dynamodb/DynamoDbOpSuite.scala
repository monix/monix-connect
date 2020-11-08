package monix.connect.dynamodb

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.JavaConverters._

@deprecated
class DynamoDbOpSuite
  extends AnyWordSpecLike with Matchers with DynamoDbFixture with BeforeAndAfterAll {

  s"${DynamoDbOp} exposes a create method" that {

    import monix.connect.dynamodb.DynamoDbOp.Implicits._

    "defines the execution of any DynamoDb request" in {
      //given
      val city = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val citizenId = Gen.nonEmptyListOf(Gen.alphaChar).sample.get.mkString
      val age = Gen.choose(0, 10000).sample.get
      val request: PutItemRequest = putItemRequest(tableName, city, citizenId, age)

      //when
      val t: Task[PutItemResponse] = DynamoDbOp.create(request)

      //then
      t.runSyncUnsafe() shouldBe a[PutItemResponse]
      val getResponse: GetItemResponse = Task.from(client.getItem(getItemRequest(tableName, city, citizenId))).runSyncUnsafe()
      getResponse.item().values().asScala.head.n().toDouble shouldBe age
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
