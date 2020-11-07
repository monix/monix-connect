package monix.connect.dynamodb

import java.lang.Thread.sleep

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.services.dynamodb.model._

import scala.collection.JavaConverters._

class DynamoDbSuite extends AnyFlatSpec with Matchers with DynamoDbFixture with BeforeAndAfterAll {

  import monix.connect.dynamodb.DynamoDbOp.Implicits._
  s"${DynamoDbOp}" can "be created from config" in {
    //given
    val citizenId = Gen.identifier.sample.get
    val city = Gen.identifier.sample.get
    val debt = Gen.choose(0, 10000).sample.get
    val request: PutItemRequest = putItemRequest(tableName, citizenId, city, debt)

    //when
    val t: Task[PutItemResponse] = DynamoDbOp.create(request)

    //then
    t.runSyncUnsafe() shouldBe a[PutItemResponse]
    val getResponse = Task.from(client.getItem(getItemRequest(tableName, citizenId, city))).runSyncUnsafe()
    getResponse.item().values().asScala.head.n().toDouble shouldBe debt
  }

  override def beforeAll(): Unit = {
    createTable(tableName)
    sleep(3000) // giving some time after table create to avoid pipeline failures
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
