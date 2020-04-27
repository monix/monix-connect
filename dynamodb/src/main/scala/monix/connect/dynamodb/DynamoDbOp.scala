/*
 * Copyright (c) 2014-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monix.connect.dynamodb

import java.util.concurrent.CompletableFuture

import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemRequest,
  CreateTableRequest,
  CreateTableResponse,
  DeleteTableResponse,
  PutItemResponse
}
//import com.amazonaws.services.dynamodbv2.model.{BatchGetItemRequest, BatchGetItemResult, BatchWriteItemRequest, BatchWriteItemResult}
//import com.amazonaws.{AmazonWebServiceRequest, AmazonWebServiceResponse, ResponseMetadata}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemResponse,
  BatchWriteItemRequest,
  BatchWriteItemResponse,
  DeleteTableRequest,
  DeleteTableResponse,
  DynamoDbRequest,
  DynamoDbResponse,
  GetItemRequest,
  GetItemResponse,
  PutItemRequest
}

sealed trait DynamoDbOp[In <: DynamoDbRequest, Out <: DynamoDbResponse] {
  def execute(dynamoDbRequest: In)(implicit client: DynamoDbAsyncClient): CompletableFuture[Out]
}

object DynamoDbOp {

  implicit val createTableOp = new DynamoDbOp[CreateTableRequest, CreateTableResponse] {
    def execute(request: software.amazon.awssdk.services.dynamodb.model.CreateTableRequest)(
      implicit
      client: DynamoDbAsyncClient): CompletableFuture[CreateTableResponse] = {
      client.createTable(request)
    }
  }

  implicit val deleteTableOp = new DynamoDbOp[DeleteTableRequest, DeleteTableResponse] {
    def execute(request: DeleteTableRequest)(
      implicit
      client: DynamoDbAsyncClient): CompletableFuture[DeleteTableResponse] = {
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
      implicit
      client: DynamoDbAsyncClient): CompletableFuture[BatchGetItemResponse] = {
      client.batchGetItem(request)
    }
  }

  implicit val batchWriteOp = new DynamoDbOp[BatchWriteItemRequest, BatchWriteItemResponse] {
    def execute(request: BatchWriteItemRequest)(
      implicit
      client: DynamoDbAsyncClient): CompletableFuture[BatchWriteItemResponse] = {
      client.batchWriteItem(request)
    }
  }

}
