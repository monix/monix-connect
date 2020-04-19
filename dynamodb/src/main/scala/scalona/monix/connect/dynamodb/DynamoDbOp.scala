package scalona.monix.connect.dynamodb

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.dynamodb.model.{ BatchGetItemRequest, CreateTableRequest, CreateTableResponse, DeleteTableResponse, PutItemResponse }
//import com.amazonaws.services.dynamodbv2.model.{BatchGetItemRequest, BatchGetItemResult, BatchWriteItemRequest, BatchWriteItemResult}
//import com.amazonaws.{AmazonWebServiceRequest, AmazonWebServiceResponse, ResponseMetadata}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{ BatchGetItemResponse, BatchWriteItemRequest, BatchWriteItemResponse, DeleteTableRequest, DeleteTableResponse, DynamoDbRequest, DynamoDbResponse, GetItemRequest, GetItemResponse, PutItemRequest }

sealed trait DynamoDbOp[In <: DynamoDbRequest, Out <: DynamoDbResponse] {
  def execute(dynamoDbRequest: In)(implicit client: DynamoDbAsyncClient): CompletableFuture[Out]
}

object DynamoDbOp {

  implicit val createTableOp = new DynamoDbOp[CreateTableRequest, CreateTableResponse] {
    def execute(request: software.amazon.awssdk.services.dynamodb.model.CreateTableRequest)(
      implicit client: DynamoDbAsyncClient): CompletableFuture[CreateTableResponse] = {
      client.createTable(request)
    }
  }

  implicit val deleteTableOp = new DynamoDbOp[DeleteTableRequest, DeleteTableResponse] {
    def execute(request: DeleteTableRequest)(
      implicit client: DynamoDbAsyncClient): CompletableFuture[DeleteTableResponse] = {
      client.deleteTable(request)
    }
  }

  implicit val putItemOp = new DynamoDbOp[PutItemRequest, PutItemResponse] {
    def execute(request: PutItemRequest)(implicit client: DynamoDbAsyncClient): CompletableFuture[PutItemResponse] = {
      client.putItem(request)
    }
  }

  implicit val getItemOp = new DynamoDbOp[GetItemRequest, GetItemResponse] {
    def execute(request: GetItemRequest)(implicit client: DynamoDbAsyncClient): CompletableFuture[GetItemResponse] = {
      client.getItem(request)
    }
  }

  implicit val batchGetOp = new DynamoDbOp[BatchGetItemRequest, BatchGetItemResponse] {
    def execute(request: BatchGetItemRequest)(
      implicit client: DynamoDbAsyncClient): CompletableFuture[BatchGetItemResponse] = {
      client.batchGetItem(request)
    }
  }

  implicit val batchWriteOp = new DynamoDbOp[BatchWriteItemRequest, BatchWriteItemResponse] {
    def execute(request: BatchWriteItemRequest)(
      implicit client: DynamoDbAsyncClient): CompletableFuture[BatchWriteItemResponse] = {
      client.batchWriteItem(request)
    }
  }

}
