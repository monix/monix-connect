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
import software.amazon.awssdk.services.dynamodb.model.{BatchGetItemRequest, BatchGetItemResponse, BatchWriteItemRequest, BatchWriteItemResponse, CreateBackupRequest, CreateBackupResponse, CreateGlobalTableRequest, CreateGlobalTableResponse, CreateTableRequest, CreateTableResponse, DeleteBackupRequest, DeleteBackupResponse, DeleteItemRequest, DeleteItemResponse, DeleteTableRequest, DeleteTableResponse, DescribeBackupRequest, DescribeBackupResponse, DescribeContinuousBackupsRequest, DescribeContinuousBackupsResponse, DescribeContributorInsightsRequest, DescribeContributorInsightsResponse, DescribeEndpointsRequest, DescribeEndpointsResponse, DescribeGlobalTableRequest, DescribeGlobalTableResponse, DescribeGlobalTableSettingsRequest, DescribeGlobalTableSettingsResponse, DescribeLimitsRequest, DescribeLimitsResponse, DescribeTableReplicaAutoScalingRequest, DescribeTableReplicaAutoScalingResponse, DescribeTimeToLiveRequest, DescribeTimeToLiveResponse, DynamoDbRequest, DynamoDbResponse, GetItemRequest, GetItemResponse, ListBackupsRequest, ListBackupsResponse, ListContributorInsightsRequest, ListContributorInsightsResponse, ListGlobalTablesRequest, ListGlobalTablesResponse, ListTablesRequest, ListTablesResponse, ListTagsOfResourceRequest, ListTagsOfResourceResponse, PutItemRequest, PutItemResponse, QueryRequest, QueryResponse, RestoreTableFromBackupRequest, RestoreTableFromBackupResponse, RestoreTableToPointInTimeRequest, RestoreTableToPointInTimeResponse, ScanRequest, ScanResponse, TagResourceRequest, TagResourceResponse, TransactGetItemsRequest, TransactGetItemsResponse, TransactWriteItemsRequest, TransactWriteItemsResponse, UntagResourceRequest, UntagResourceResponse, UpdateContinuousBackupsRequest, UpdateContinuousBackupsResponse, UpdateContributorInsightsRequest, UpdateContributorInsightsResponse, UpdateGlobalTableRequest, UpdateGlobalTableResponse, UpdateGlobalTableSettingsRequest, UpdateGlobalTableSettingsResponse, UpdateItemRequest, UpdateItemResponse, UpdateTableReplicaAutoScalingRequest, UpdateTableReplicaAutoScalingResponse, UpdateTableRequest, UpdateTableResponse, UpdateTimeToLiveRequest, UpdateTimeToLiveResponse}
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
      DynamoDbOpFactory.build[BatchGetItemRequest, BatchGetItemResponse](_.batchGetItem(_))
    implicit val batchWriteOp =
      DynamoDbOpFactory.build[BatchWriteItemRequest, BatchWriteItemResponse](_.batchWriteItem(_))
    implicit val createBackupOp =
      DynamoDbOpFactory.build[CreateBackupRequest, CreateBackupResponse](_.createBackup(_))
    implicit val createGlobalTableOp =
      DynamoDbOpFactory.build[CreateGlobalTableRequest, CreateGlobalTableResponse](_.createGlobalTable(_))
    implicit val createTableOp =
      DynamoDbOpFactory.build[CreateTableRequest, CreateTableResponse](_.createTable(_))
    implicit val deleteBackupOp =
      DynamoDbOpFactory.build[DeleteBackupRequest, DeleteBackupResponse](_.deleteBackup(_))
    implicit val deleteItemOp = DynamoDbOpFactory.build[DeleteItemRequest, DeleteItemResponse](_.deleteItem(_))
    implicit val describeBackupOp =
      DynamoDbOpFactory.build[DescribeBackupRequest, DescribeBackupResponse](_.describeBackup(_))
    implicit val describeContinuousBackupsOp = DynamoDbOpFactory
      .build[DescribeContinuousBackupsRequest, DescribeContinuousBackupsResponse](_.describeContinuousBackups(_))
    implicit val describeContributorInsightsOp = DynamoDbOpFactory
      .build[DescribeContributorInsightsRequest, DescribeContributorInsightsResponse](_.describeContributorInsights(_))
    implicit val describeEndpointsOp =
      DynamoDbOpFactory.build[DescribeEndpointsRequest, DescribeEndpointsResponse](_.describeEndpoints(_))
    implicit val describeGlobalTableOp =
      DynamoDbOpFactory.build[DescribeGlobalTableRequest, DescribeGlobalTableResponse](_.describeGlobalTable(_))
    implicit val describeGlobalTableSettingsOp = DynamoDbOpFactory
      .build[DescribeGlobalTableSettingsRequest, DescribeGlobalTableSettingsResponse](_.describeGlobalTableSettings(_))
    implicit val describeLimitsOp =
      DynamoDbOpFactory.build[DescribeLimitsRequest, DescribeLimitsResponse](_.describeLimits(_))
    implicit val describeTableReplicaAutoScalingRequest =
      DynamoDbOpFactory.build[DescribeTableReplicaAutoScalingRequest, DescribeTableReplicaAutoScalingResponse](
        _.describeTableReplicaAutoScaling(_))
    implicit val deleteTableOp =
      DynamoDbOpFactory.build[DeleteTableRequest, DeleteTableResponse](_.deleteTable(_))
    implicit val describeTimeToLiveOp =
      DynamoDbOpFactory.build[DescribeTimeToLiveRequest, DescribeTimeToLiveResponse](_.describeTimeToLive(_))
    implicit val getItemOp = DynamoDbOpFactory.build[GetItemRequest, GetItemResponse](_.getItem(_))
    implicit val listBackupsOp =
      DynamoDbOpFactory.build[ListBackupsRequest, ListBackupsResponse](_.listBackups(_))
    implicit val listContributorInsightsOp = DynamoDbOpFactory
      .build[ListContributorInsightsRequest, ListContributorInsightsResponse](_.listContributorInsights(_))
    implicit val listTablesOp = DynamoDbOpFactory.build[ListTablesRequest, ListTablesResponse](_.listTables(_))
    implicit val listGlobalTablesOp =
      DynamoDbOpFactory.build[ListGlobalTablesRequest, ListGlobalTablesResponse](_.listGlobalTables(_))
    implicit val listTagsOfResourceOp =
      DynamoDbOpFactory.build[ListTagsOfResourceRequest, ListTagsOfResourceResponse](_.listTagsOfResource(_))
    implicit val putItemOp = DynamoDbOpFactory.build[PutItemRequest, PutItemResponse](_.putItem(_))
    implicit val queryOp = DynamoDbOpFactory.build[QueryRequest, QueryResponse](_.query(_))
    implicit val restoreTableFromBackupOp =
      DynamoDbOpFactory.build[RestoreTableFromBackupRequest, RestoreTableFromBackupResponse](
        _.restoreTableFromBackup(_))
    implicit val restoreTableToPointInTimeOp = DynamoDbOpFactory
      .build[RestoreTableToPointInTimeRequest, RestoreTableToPointInTimeResponse](_.restoreTableToPointInTime(_))
    implicit val scanOp = DynamoDbOpFactory.build[ScanRequest, ScanResponse](_.scan(_))
    implicit val tagResourceOp =
      DynamoDbOpFactory.build[TagResourceRequest, TagResourceResponse](_.tagResource(_))
    implicit val transactGetItemsOp =
      DynamoDbOpFactory.build[TransactGetItemsRequest, TransactGetItemsResponse](_.transactGetItems(_))
    implicit val transactWriteItemsOp =
      DynamoDbOpFactory.build[TransactWriteItemsRequest, TransactWriteItemsResponse](_.transactWriteItems(_))
    implicit val untagResourceRequest =
      DynamoDbOpFactory.build[UntagResourceRequest, UntagResourceResponse](_.untagResource(_))
    implicit val updateContinuousBackupsOp = DynamoDbOpFactory
      .build[UpdateContinuousBackupsRequest, UpdateContinuousBackupsResponse](_.updateContinuousBackups(_))
    implicit val updateContributorInsightsOp = DynamoDbOpFactory
      .build[UpdateContributorInsightsRequest, UpdateContributorInsightsResponse](_.updateContributorInsights(_))
    implicit val updateGlobalTableOp =
      DynamoDbOpFactory.build[UpdateGlobalTableRequest, UpdateGlobalTableResponse](_.updateGlobalTable(_))
    implicit val updateGlobalTableSettingsOp = DynamoDbOpFactory
      .build[UpdateGlobalTableSettingsRequest, UpdateGlobalTableSettingsResponse](_.updateGlobalTableSettings(_))
    implicit val updateItemOp = DynamoDbOpFactory.build[UpdateItemRequest, UpdateItemResponse](_.updateItem(_))
    implicit val updateTableReplicaAutoScalingOp =
      DynamoDbOpFactory.build[UpdateTableReplicaAutoScalingRequest, UpdateTableReplicaAutoScalingResponse](
        _.updateTableReplicaAutoScaling(_))
    implicit val updateTableOp =
      DynamoDbOpFactory.build[UpdateTableRequest, UpdateTableResponse](_.updateTable(_))
    implicit val updateTimeToLiveOp =
      DynamoDbOpFactory.build[UpdateTimeToLiveRequest, UpdateTimeToLiveResponse](_.updateTimeToLive(_))
  }

  def getItem(
    tableName: String,
    projectionExpression: Option[String] = None,
    consistentRead: Boolean = true,
    expressionAttributeNames: Map[String, String] = Map.empty,
    consumedCapacityDetail: ConsumedCapacity.Detail = ConsumedCapacity.NONE,
    retryableSettings: RetrySettings = domain.DefaultRetrySettings)(
    implicit asyncClient: DynamoDbAsyncClient): Task[GetItemResponse] = {
    import Implicits.getItemOp
    val getItemRequest =
      RequestsBuilder.getItemRequest(
        tableName,
        projectionExpression,
        consistentRead,
        expressionAttributeNames,
        consumedCapacityDetail)
    create(getItemRequest, retryableSettings.retries, retryableSettings.delayAfterFailure)
  }
  
  /**
    * A factory for avoiding boilerplate when building specific [[DynamoDbOp]].
    */
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
