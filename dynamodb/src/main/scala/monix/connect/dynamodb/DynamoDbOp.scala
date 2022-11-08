/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
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

import scala.concurrent.duration.FiniteDuration

/**
  * Abstracts the execution of any given [[DynamoDbRequest]] with its correspondent operation that returns [[DynamoDbResponse]].
  * @tparam In The input request as type parameter lower bounded by [[DynamoDbRequest]].
  * @tparam Out The response of the execution as type parameter lower bounded by [[DynamoDbResponse]].
  */
private[dynamodb] trait DynamoDbOp[In <: DynamoDbRequest, Out <: DynamoDbResponse] {
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

    implicit val batchGetOp: DynamoDbOp[BatchGetItemRequest, BatchGetItemResponse] =
      DynamoDbOpFactory.build[BatchGetItemRequest, BatchGetItemResponse](_.batchGetItem(_))
    implicit val batchWriteOp: DynamoDbOp[BatchWriteItemRequest, BatchWriteItemResponse] =
      DynamoDbOpFactory.build[BatchWriteItemRequest, BatchWriteItemResponse](_.batchWriteItem(_))
    implicit val createBackupOp: DynamoDbOp[CreateBackupRequest, CreateBackupResponse] =
      DynamoDbOpFactory.build[CreateBackupRequest, CreateBackupResponse](_.createBackup(_))
    implicit val createGlobalTableOp: DynamoDbOp[CreateGlobalTableRequest, CreateGlobalTableResponse] =
      DynamoDbOpFactory.build[CreateGlobalTableRequest, CreateGlobalTableResponse](_.createGlobalTable(_))
    implicit val createTableOp: DynamoDbOp[CreateTableRequest, CreateTableResponse] =
      DynamoDbOpFactory.build[CreateTableRequest, CreateTableResponse](_.createTable(_))
    implicit val deleteBackupOp: DynamoDbOp[DeleteBackupRequest, DeleteBackupResponse] =
      DynamoDbOpFactory.build[DeleteBackupRequest, DeleteBackupResponse](_.deleteBackup(_))
    implicit val deleteItemOp: DynamoDbOp[DeleteItemRequest, DeleteItemResponse] =
      DynamoDbOpFactory.build[DeleteItemRequest, DeleteItemResponse](_.deleteItem(_))
    implicit val describeBackupOp: DynamoDbOp[DescribeBackupRequest, DescribeBackupResponse] =
      DynamoDbOpFactory.build[DescribeBackupRequest, DescribeBackupResponse](_.describeBackup(_))
    implicit val describeContinuousBackupsOp
      : DynamoDbOp[DescribeContinuousBackupsRequest, DescribeContinuousBackupsResponse] = DynamoDbOpFactory
      .build[DescribeContinuousBackupsRequest, DescribeContinuousBackupsResponse](_.describeContinuousBackups(_))
    implicit val describeContributorInsightsOp
      : DynamoDbOp[DescribeContributorInsightsRequest, DescribeContributorInsightsResponse] = DynamoDbOpFactory
      .build[DescribeContributorInsightsRequest, DescribeContributorInsightsResponse](_.describeContributorInsights(_))
    implicit val describeEndpointsOp: DynamoDbOp[DescribeEndpointsRequest, DescribeEndpointsResponse] =
      DynamoDbOpFactory.build[DescribeEndpointsRequest, DescribeEndpointsResponse](_.describeEndpoints(_))
    implicit val describeGlobalTableOp: DynamoDbOp[DescribeGlobalTableRequest, DescribeGlobalTableResponse] =
      DynamoDbOpFactory.build[DescribeGlobalTableRequest, DescribeGlobalTableResponse](_.describeGlobalTable(_))
    implicit val describeGlobalTableSettingsOp
      : DynamoDbOp[DescribeGlobalTableSettingsRequest, DescribeGlobalTableSettingsResponse] = DynamoDbOpFactory
      .build[DescribeGlobalTableSettingsRequest, DescribeGlobalTableSettingsResponse](_.describeGlobalTableSettings(_))
    implicit val describeLimitsOp: DynamoDbOp[DescribeLimitsRequest, DescribeLimitsResponse] =
      DynamoDbOpFactory.build[DescribeLimitsRequest, DescribeLimitsResponse](_.describeLimits(_))
    implicit val describeTableReplicaAutoScalingRequest
      : DynamoDbOp[DescribeTableReplicaAutoScalingRequest, DescribeTableReplicaAutoScalingResponse] =
      DynamoDbOpFactory.build[DescribeTableReplicaAutoScalingRequest, DescribeTableReplicaAutoScalingResponse](
        _.describeTableReplicaAutoScaling(_))
    implicit val deleteTableOp: DynamoDbOp[DeleteTableRequest, DeleteTableResponse] =
      DynamoDbOpFactory.build[DeleteTableRequest, DeleteTableResponse](_.deleteTable(_))
    implicit val describeTimeToLiveOp: DynamoDbOp[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse] =
      DynamoDbOpFactory.build[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse](_.describeTimeToLive(_))
    implicit val getItemOp: DynamoDbOp[GetItemRequest, GetItemResponse] =
      DynamoDbOpFactory.build[GetItemRequest, GetItemResponse](_.getItem(_))
    implicit val listBackupsOp: DynamoDbOp[ListBackupsRequest, ListBackupsResponse] =
      DynamoDbOpFactory.build[ListBackupsRequest, ListBackupsResponse](_.listBackups(_))
    implicit val listContributorInsightsOp
      : DynamoDbOp[ListContributorInsightsRequest, ListContributorInsightsResponse] = DynamoDbOpFactory
      .build[ListContributorInsightsRequest, ListContributorInsightsResponse](_.listContributorInsights(_))
    implicit val listTablesOp: DynamoDbOp[ListTablesRequest, ListTablesResponse] =
      DynamoDbOpFactory.build[ListTablesRequest, ListTablesResponse](_.listTables(_))
    implicit val listGlobalTablesOp: DynamoDbOp[ListGlobalTablesRequest, ListGlobalTablesResponse] =
      DynamoDbOpFactory.build[ListGlobalTablesRequest, ListGlobalTablesResponse](_.listGlobalTables(_))
    implicit val listTagsOfResourceOp: DynamoDbOp[ListTagsOfResourceRequest, ListTagsOfResourceResponse] =
      DynamoDbOpFactory.build[ListTagsOfResourceRequest, ListTagsOfResourceResponse](_.listTagsOfResource(_))
    implicit val putItemOp: DynamoDbOp[PutItemRequest, PutItemResponse] =
      DynamoDbOpFactory.build[PutItemRequest, PutItemResponse](_.putItem(_))
    implicit val queryOp: DynamoDbOp[QueryRequest, QueryResponse] =
      DynamoDbOpFactory.build[QueryRequest, QueryResponse](_.query(_))
    implicit val restoreTableFromBackupOp: DynamoDbOp[RestoreTableFromBackupRequest, RestoreTableFromBackupResponse] =
      DynamoDbOpFactory.build[RestoreTableFromBackupRequest, RestoreTableFromBackupResponse](
        _.restoreTableFromBackup(_))
    implicit val restoreTableToPointInTimeOp
      : DynamoDbOp[RestoreTableToPointInTimeRequest, RestoreTableToPointInTimeResponse] = DynamoDbOpFactory
      .build[RestoreTableToPointInTimeRequest, RestoreTableToPointInTimeResponse](_.restoreTableToPointInTime(_))
    implicit val scanOp: DynamoDbOp[ScanRequest, ScanResponse] =
      DynamoDbOpFactory.build[ScanRequest, ScanResponse](_.scan(_))
    implicit val tagResourceOp: DynamoDbOp[TagResourceRequest, TagResourceResponse] =
      DynamoDbOpFactory.build[TagResourceRequest, TagResourceResponse](_.tagResource(_))
    implicit val transactGetItemsOp: DynamoDbOp[TransactGetItemsRequest, TransactGetItemsResponse] =
      DynamoDbOpFactory.build[TransactGetItemsRequest, TransactGetItemsResponse](_.transactGetItems(_))
    implicit val transactWriteItemsOp: DynamoDbOp[TransactWriteItemsRequest, TransactWriteItemsResponse] =
      DynamoDbOpFactory.build[TransactWriteItemsRequest, TransactWriteItemsResponse](_.transactWriteItems(_))
    implicit val untagResourceRequest: DynamoDbOp[UntagResourceRequest, UntagResourceResponse] =
      DynamoDbOpFactory.build[UntagResourceRequest, UntagResourceResponse](_.untagResource(_))
    implicit val updateContinuousBackupsOp
      : DynamoDbOp[UpdateContinuousBackupsRequest, UpdateContinuousBackupsResponse] = DynamoDbOpFactory
      .build[UpdateContinuousBackupsRequest, UpdateContinuousBackupsResponse](_.updateContinuousBackups(_))
    implicit val updateContributorInsightsOp
      : DynamoDbOp[UpdateContributorInsightsRequest, UpdateContributorInsightsResponse] = DynamoDbOpFactory
      .build[UpdateContributorInsightsRequest, UpdateContributorInsightsResponse](_.updateContributorInsights(_))
    implicit val updateGlobalTableOp: DynamoDbOp[UpdateGlobalTableRequest, UpdateGlobalTableResponse] =
      DynamoDbOpFactory.build[UpdateGlobalTableRequest, UpdateGlobalTableResponse](_.updateGlobalTable(_))
    implicit val updateGlobalTableSettingsOp
      : DynamoDbOp[UpdateGlobalTableSettingsRequest, UpdateGlobalTableSettingsResponse] = DynamoDbOpFactory
      .build[UpdateGlobalTableSettingsRequest, UpdateGlobalTableSettingsResponse](_.updateGlobalTableSettings(_))
    implicit val updateItemOp: DynamoDbOp[UpdateItemRequest, UpdateItemResponse] =
      DynamoDbOpFactory.build[UpdateItemRequest, UpdateItemResponse](_.updateItem(_))
    implicit val updateTableReplicaAutoScalingOp
      : DynamoDbOp[UpdateTableReplicaAutoScalingRequest, UpdateTableReplicaAutoScalingResponse] =
      DynamoDbOpFactory.build[UpdateTableReplicaAutoScalingRequest, UpdateTableReplicaAutoScalingResponse](
        _.updateTableReplicaAutoScaling(_))
    implicit val updateTableOp: DynamoDbOp[UpdateTableRequest, UpdateTableResponse] =
      DynamoDbOpFactory.build[UpdateTableRequest, UpdateTableResponse](_.updateTable(_))
    implicit val updateTimeToLiveOp: DynamoDbOp[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse] =
      DynamoDbOpFactory.build[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse](_.updateTimeToLive(_))
  }

  /** A factory for avoiding boilerplate when building specific [[DynamoDbOp]]. */
  private[this] object DynamoDbOpFactory {
    def build[Req <: DynamoDbRequest, Resp <: DynamoDbResponse](
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

  @deprecated("Use `single` from DynamoDb")
  final def create[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    request: In,
    retries: Int = 0,
    delayAfterFailure: Option[FiniteDuration] = None)(
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient): Task[Out] = {

    require(retries >= 0, "Retries per operation must be higher or equal than 0.")

    Task
      .defer(dynamoDbOp(request))
      .onErrorHandleWith { ex =>
        val t = Task
          .defer(
            if (retries > 0) create(request, retries - 1, delayAfterFailure)
            else Task.raiseError(ex))
        delayAfterFailure match {
          case Some(delay) => t.delayExecution(delay)
          case None => t
        }
      }
  }

}
