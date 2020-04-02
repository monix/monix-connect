package scalona.monix.connectors.dynamodb

import java.util.concurrent.CompletableFuture

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import scalona.monix.connectors.common.Implicits.Transformer
import scalona.monix.connectors.common.Implicits._

import scala.concurrent.Future

class DynamoDbTransformerSpec extends WordSpecLike with Matchers with ScalaFutures with DynamoDbFixture with BeforeAndAfterAll {

  /*val key = AttributeValue.builder().s("key1").build()
  val value = AttributeValue.builder().n("1").build()
  val keyMap = Map("keyCol" -> key, "valCol" -> value)
  val getItemRequest: GetItemRequest =
    GetItemRequest.builder().tableName("tableName").key(keyMap.asJava).attributesToGet("data").build()
   */

  implicit val client: DynamoDbAsyncClient = DynamoDbClient()
  s"${DynamoDb}.transformer() " should {

    s"create a reactive `Transformer`" that {

      s"receives `CreateTableRequests` and returns `CreateTableResponses`" in {
        //given
        val transformer: Transformer[CreateTableRequest, Future[CreateTableResponse]] =
          DynamoDb.transformer[CreateTableRequest, CreateTableResponse]
        val request = createTableRequest(tableName = citiesTableName, schema = cityKeySchema, attributeDefinition = cityAttrDef)

        //when
        val ob: Observable[Future[CreateTableResponse]] =
          Observable
            .fromIterable(Iterable(request))
            .transform(transformer)
        val f: Future[CreateTableResponse] = ob.headL.runSyncUnsafe()

        //then
        whenReady(f) { response: CreateTableResponse =>
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
  }


  override def beforeAll(): Unit = {
    super.beforeAll()
    deleteTable(citiesTableName)
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
