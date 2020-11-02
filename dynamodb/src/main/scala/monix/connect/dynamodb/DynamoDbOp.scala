/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
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

import monix.connect.dynamodb.domain.RetrySettings
import monix.eval.Task
import software.amazon.awssdk.services.dynamodb.model.{
  BatchGetItemRequest,
  BatchGetItemResponse,
  BatchWriteItemRequest,
  BatchWriteItemResponse,
  CreateBackupRequest,
  CreateBackupResponse,
  CreateGlobalTableRequest,
  CreateGlobalTableResponse,
  CreateTableRequest,
  CreateTableResponse,
  DeleteBackupRequest,
  DeleteBackupResponse,
  DeleteItemRequest,
  DeleteItemResponse,
  DeleteTableRequest,
  DeleteTableResponse,
  DescribeBackupRequest,
  DescribeBackupResponse,
  DescribeContinuousBackupsRequest,
  DescribeContinuousBackupsResponse,
  DescribeContributorInsightsRequest,
  DescribeContributorInsightsResponse,
  DescribeEndpointsRequest,
  DescribeEndpointsResponse,
  DescribeGlobalTableRequest,
  DescribeGlobalTableResponse,
  DescribeGlobalTableSettingsRequest,
  DescribeGlobalTableSettingsResponse,
  DescribeLimitsRequest,
  DescribeLimitsResponse,
  DescribeTableReplicaAutoScalingRequest,
  DescribeTableReplicaAutoScalingResponse,
  DescribeTimeToLiveRequest,
  DescribeTimeToLiveResponse,
  DynamoDbRequest,
  DynamoDbResponse,
  GetItemRequest,
  GetItemResponse,
  ListBackupsRequest,
  ListBackupsResponse,
  ListContributorInsightsRequest,
  ListContributorInsightsResponse,
  ListGlobalTablesRequest,
  ListGlobalTablesResponse,
  ListTablesRequest,
  ListTablesResponse,
  ListTagsOfResourceRequest,
  ListTagsOfResourceResponse,
  PutItemRequest,
  PutItemResponse,
  QueryRequest,
  QueryResponse,
  RestoreTableFromBackupRequest,
  RestoreTableFromBackupResponse,
  RestoreTableToPointInTimeRequest,
  RestoreTableToPointInTimeResponse,
  ScanRequest,
  ScanResponse,
  TagResourceRequest,
  TagResourceResponse,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse,
  UntagResourceRequest,
  UntagResourceResponse,
  UpdateContinuousBackupsRequest,
  UpdateContinuousBackupsResponse,
  UpdateContributorInsightsRequest,
  UpdateContributorInsightsResponse,
  UpdateGlobalTableRequest,
  UpdateGlobalTableResponse,
  UpdateGlobalTableSettingsRequest,
  UpdateGlobalTableSettingsResponse,
  UpdateItemRequest,
  UpdateItemResponse,
  UpdateTableReplicaAutoScalingRequest,
  UpdateTableReplicaAutoScalingResponse,
  UpdateTableRequest,
  UpdateTableResponse,
  UpdateTimeToLiveRequest,
  UpdateTimeToLiveResponse
}
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

import scala.language.implicitConversions
import scala.concurrent.duration.FiniteDuration

/**
  * Abstracts the execution of any given [[DynamoDbRequest]] with its correspondent operation that returns [[DynamoDbResponse]].
  * @tparam In The input request as type parameter lower bounded by [[DynamoDbRequest]].
  * @tparam Out The response of the execution as type parameter lower bounded by [[DynamoDbResponse]].
  */
trait DynamoDbOp[In <: DynamoDbRequest, Out <: DynamoDbResponse] {
  def apply(dynamoDbRequest: In)(implicit client: DynamoDbAsyncClient): Task[Out] =
    Task.defer(Task.from(execute(dynamoDbRequest)))
  def execute(dynamoDbRequest: In)(implicit client: DynamoDbAsyncClient): CompletableFuture[Out]
}

/**
  * Defines all the available dynamodb operations available from [[DynamoDbAsyncClient]] as a [[DynamoDbOp]].
  * @note All of them are defined implicitly, and can be imported from the object [[DynamoDbOp.Implicits]]
  *       which will automatically infer and extend the [[DynamoDbRequest]] as [[DynamoDbOp]].
  */
object DynamoDbOp {

  object Implicits {

    implicit val batchGetOp =
      DynamoDbOpFactory.create[BatchGetItemRequest, BatchGetItemResponse](_.batchGetItem(_))
    implicit val batchWriteOp =
      DynamoDbOpFactory.create[BatchWriteItemRequest, BatchWriteItemResponse](_.batchWriteItem(_))
    implicit val createBackupOp =
      DynamoDbOpFactory.create[CreateBackupRequest, CreateBackupResponse](_.createBackup(_))
    implicit val createGlobalTableOp =
      DynamoDbOpFactory.create[CreateGlobalTableRequest, CreateGlobalTableResponse](_.createGlobalTable(_))
    implicit val createTableOp =
      DynamoDbOpFactory.create[CreateTableRequest, CreateTableResponse](_.createTable(_))
    implicit val deleteBackupOp =
      DynamoDbOpFactory.create[DeleteBackupRequest, DeleteBackupResponse](_.deleteBackup(_))
    implicit val deleteItemOp = DynamoDbOpFactory.create[DeleteItemRequest, DeleteItemResponse](_.deleteItem(_))
    implicit val describeBackupOp =
      DynamoDbOpFactory.create[DescribeBackupRequest, DescribeBackupResponse](_.describeBackup(_))
    implicit val describeContinuousBackupsOp = DynamoDbOpFactory
      .create[DescribeContinuousBackupsRequest, DescribeContinuousBackupsResponse](_.describeContinuousBackups(_))
    implicit val describeContributorInsightsOp = DynamoDbOpFactory
      .create[DescribeContributorInsightsRequest, DescribeContributorInsightsResponse](_.describeContributorInsights(_))
    implicit val describeEndpointsOp =
      DynamoDbOpFactory.create[DescribeEndpointsRequest, DescribeEndpointsResponse](_.describeEndpoints(_))
    implicit val describeGlobalTableOp =
      DynamoDbOpFactory.create[DescribeGlobalTableRequest, DescribeGlobalTableResponse](_.describeGlobalTable(_))
    implicit val describeGlobalTableSettingsOp = DynamoDbOpFactory
      .create[DescribeGlobalTableSettingsRequest, DescribeGlobalTableSettingsResponse](_.describeGlobalTableSettings(_))
    implicit val describeLimitsOp =
      DynamoDbOpFactory.create[DescribeLimitsRequest, DescribeLimitsResponse](_.describeLimits(_))
    implicit val describeTableReplicaAutoScalingRequest =
      DynamoDbOpFactory.create[DescribeTableReplicaAutoScalingRequest, DescribeTableReplicaAutoScalingResponse](
        _.describeTableReplicaAutoScaling(_))
    implicit val deleteTableOp =
      DynamoDbOpFactory.create[DeleteTableRequest, DeleteTableResponse](_.deleteTable(_))
    implicit val describeTimeToLiveOp =
      DynamoDbOpFactory.create[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse](_.describeTimeToLive(_))
    implicit val getItemOp = DynamoDbOpFactory.create[GetItemRequest, GetItemResponse](_.getItem(_))
    implicit val listBackupsOp =
      DynamoDbOpFactory.create[ListBackupsRequest, ListBackupsResponse](_.listBackups(_))
    implicit val listContributorInsightsOp = DynamoDbOpFactory
      .create[ListContributorInsightsRequest, ListContributorInsightsResponse](_.listContributorInsights(_))
    implicit val listTablesOp = DynamoDbOpFactory.create[ListTablesRequest, ListTablesResponse](_.listTables(_))
    implicit val listGlobalTablesOp =
      DynamoDbOpFactory.create[ListGlobalTablesRequest, ListGlobalTablesResponse](_.listGlobalTables(_))
    implicit val listTagsOfResourceOp =
      DynamoDbOpFactory.create[ListTagsOfResourceRequest, ListTagsOfResourceResponse](_.listTagsOfResource(_))
    implicit val putItemOp = DynamoDbOpFactory.create[PutItemRequest, PutItemResponse](_.putItem(_))
    implicit val queryOp = DynamoDbOpFactory.create[QueryRequest, QueryResponse](_.query(_))
    implicit val restoreTableFromBackupOp =
      DynamoDbOpFactory.create[RestoreTableFromBackupRequest, RestoreTableFromBackupResponse](
        _.restoreTableFromBackup(_))
    implicit val restoreTableToPointInTimeOp = DynamoDbOpFactory
      .create[RestoreTableToPointInTimeRequest, RestoreTableToPointInTimeResponse](_.restoreTableToPointInTime(_))
    implicit val scanOp = DynamoDbOpFactory.create[ScanRequest, ScanResponse](_.scan(_))
    implicit val tagResourceOp =
      DynamoDbOpFactory.create[TagResourceRequest, TagResourceResponse](_.tagResource(_))
    implicit val transactGetItemOp =
      DynamoDbOpFactory.create[TransactGetItemRequest, TransactGetItemsResponse](_.tra(_))
    implicit val transactGetItemsOp =
      DynamoDbOpFactory.create[TransactGetItemsRequest, TransactGetItemsResponse](_.transactGetItems(_))
    implicit val transactWriteItemsOp =
      DynamoDbOpFactory.create[TransactWriteItemsRequest, TransactWriteItemsResponse](_.transactWriteItems(_))
    implicit val untagResourceRequest =
      DynamoDbOpFactory.create[UntagResourceRequest, UntagResourceResponse](_.untagResource(_))
    implicit val updateContinuousBackupsOp = DynamoDbOpFactory
      .create[UpdateContinuousBackupsRequest, UpdateContinuousBackupsResponse](_.updateContinuousBackups(_))
    implicit val updateContributorInsightsOp = DynamoDbOpFactory
      .create[UpdateContributorInsightsRequest, UpdateContributorInsightsResponse](_.updateContributorInsights(_))
    implicit val updateGlobalTableOp =
      DynamoDbOpFactory.create[UpdateGlobalTableRequest, UpdateGlobalTableResponse](_.updateGlobalTable(_))
    implicit val updateGlobalTableSettingsOp = DynamoDbOpFactory
      .create[UpdateGlobalTableSettingsRequest, UpdateGlobalTableSettingsResponse](_.updateGlobalTableSettings(_))
    implicit val updateItemOp = DynamoDbOpFactory.create[UpdateItemRequest, UpdateItemResponse](_.updateItem(_))
    implicit val updateTableReplicaAutoScalingOp =
      DynamoDbOpFactory.create[UpdateTableReplicaAutoScalingRequest, UpdateTableReplicaAutoScalingResponse](
        _.updateTableReplicaAutoScaling(_))
    implicit val updateTableOp =
      DynamoDbOpFactory.create[UpdateTableRequest, UpdateTableResponse](_.updateTable(_))
    implicit val updateTimeToLiveOp =
      DynamoDbOpFactory.create[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse](_.updateTimeToLive(_))
  }

  /**
    * A factory for avoiding boilerplate when building specific [[DynamoDbOp]].
    */
  private[this] object DynamoDbOpFactory {
    def create[Req <: DynamoDbRequest, Resp <: DynamoDbResponse](
      operation: (DynamoDbAsyncClient, Req) => CompletableFuture[Resp]): DynamoDbOp[Req, Resp] = {
      new DynamoDbOp[Req, Resp] {
        def execute(request: Req)(
          implicit
          client: DynamoDbAsyncClient): CompletableFuture[Resp] = {
          operation(client, request)
        }
      }
    }
  }

  /**
    * Creates the description of the execution of a single request that
    * under failure it will be retried as many times as set in [[retries]].
    *
    * @param request the [[DynamoDbRequest]] that will be executed.
    * @param retries the number of times that an operation can be retried before actually returning a failed [[Task]].
    *        it must be higher or equal than 0.
    * @param delayAfterFailure delay after failure for the execution of a single [[DynamoDbOp]].
    * @param dynamoDbOp an implicit [[DynamoDbOp]] that abstracts the execution of the specific operation.
    * @return A [[Task]] that ends successfully with the response as [[DynamoDbResponse]], or a failed one.
    */
  def create[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    request: In,
    retries: Int,
    delayAfterFailure: Option[FiniteDuration])(
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient): Task[Out] = {
    require(retries >= 0, "Retries per operation must be >= 0.")
    Task
      .defer(dynamoDbOp(request))
      .onErrorHandleWith { ex =>
        val t = Task
          .defer(
            if (retries > 0) create(request, retries, delayAfterFailure)
            else Task.raiseError(ex))
        delayAfterFailure match {
          case Some(delay) => t.delayExecution(delay)
          case None => t
        }
      }
  }

}
