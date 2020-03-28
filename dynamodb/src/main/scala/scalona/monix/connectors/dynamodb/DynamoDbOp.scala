package scalona.monix.connectors.dynamodb

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.dynamodb.model.{ BatchGetItemRequest, CreateTableRequest, CreateTableResponse }
//import com.amazonaws.services.dynamodbv2.model.{BatchGetItemRequest, BatchGetItemResult, BatchWriteItemRequest, BatchWriteItemResult}
//import com.amazonaws.{AmazonWebServiceRequest, AmazonWebServiceResponse, ResponseMetadata}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{ BatchGetItemResponse, BatchWriteItemRequest, BatchWriteItemResponse, DynamoDbRequest, DynamoDbResponse, GetItemRequest, GetItemResponse }

sealed trait DynamoDbOp[In <: DynamoDbRequest, Out <: DynamoDbResponse] {
  def execute(dynamoDbRequest: In)(implicit client: DynamoDbAsyncClient): CompletableFuture[Out]
}

object DynamoDbOp {

//  implicit val amazon: AmazonDynamoDBAsync = ???
  implicit val batchGetOp = new DynamoDbOp[BatchGetItemRequest, BatchGetItemResponse] {
    def execute(dynamoDbRequest: BatchGetItemRequest)(
      implicit client: DynamoDbAsyncClient): CompletableFuture[BatchGetItemResponse] = {
      client.batchGetItem(dynamoDbRequest)
    }
  }

  implicit val createTableOp = new DynamoDbOp[CreateTableRequest, CreateTableResponse] {
    def execute(createTableRequest: software.amazon.awssdk.services.dynamodb.model.CreateTableRequest)(
      implicit client: DynamoDbAsyncClient): CompletableFuture[CreateTableResponse] = {
      client.createTable(createTableRequest)
    }
  }

  implicit val getItemRequest = new DynamoDbOp[GetItemRequest, GetItemResponse] {
    def execute(dynamoDbRequest: GetItemRequest)(
      implicit client: DynamoDbAsyncClient): CompletableFuture[GetItemResponse] = {
      client.getItem(dynamoDbRequest)
    }
  }
  implicit val batchWriteOp = new DynamoDbOp[BatchWriteItemRequest, BatchWriteItemResponse] {
    def execute(dynamoDbRequest: BatchWriteItemRequest)(
      implicit client: DynamoDbAsyncClient): CompletableFuture[BatchWriteItemResponse] = {
      client.batchWriteItem(dynamoDbRequest)
    }
  }

}
