package scalona.monix.connectors.dynamodb

import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import monix.reactive.{Consumer, Observable}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, WordSpecLike}
import software.amazon.awssdk.services.dynamodb.model._
import org.scalatest.concurrent.ScalaFutures._

class DynamoDbConsumerSpec extends WordSpecLike with Matchers with DynamoDbFixture with BeforeAndAfterAll {

  /*val key = AttributeValue.builder().s("key1").build()
  val value = AttributeValue.builder().n("1").build()
  val keyMap = Map("keyCol" -> key, "valCol" -> value)
  val getItemRequest: GetItemRequest =
    GetItemRequest.builder().tableName("tableName").key(keyMap.asJava).attributesToGet("data").build()
   */

  s"${DynamoDbConsumer}.build() " should {

    s"create a reactive $Consumer" that {

      s"accepts a stream of `CreateTableRequest` and returns `CreateTableResponses`" in {
        //given
        val consumer: Consumer[CreateTableRequest, Observable[Task[CreateTableResponse]]] =
          DynamoDbConsumer().build[CreateTableRequest, CreateTableResponse]
        val request = createTableRequest(tableName = citiesTableName, schema = cityKeySchema, attributeDefinition = cityAttrDef)

        //when
        val stream: Task[Observable[Task[CreateTableResponse]]] =
          Observable.fromIterable(Iterable(request)).consumeWith(consumer)

        //then
        val createTableResponse: CreateTableResponse = {
          stream
            .runSyncUnsafe()
            .consumeWith(Consumer.head)
            .runSyncUnsafe()
            .runSyncUnsafe()
        }
        createTableResponse shouldBe a[CreateTableResponse]
        createTableResponse.tableDescription().hasKeySchema shouldBe true
        createTableResponse.tableDescription().hasAttributeDefinitions shouldBe true
        createTableResponse.tableDescription().hasGlobalSecondaryIndexes shouldBe false
        createTableResponse.tableDescription().hasReplicas shouldBe false
        createTableResponse.tableDescription().tableName() shouldEqual citiesTableName
        createTableResponse.tableDescription().keySchema() should contain theSameElementsAs cityKeySchema
        createTableResponse.tableDescription().attributeDefinitions() should contain theSameElementsAs cityAttrDef
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
