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

import monix.connect.dynamodb.DynamoDbOp.create
import domain.{
  CreateTableSettings,
  DefaultCreateSettings,
  DefaultPutItemSettings,
  DefaultRestoreTableFromBackupSettings,
  DefaultRestoreTableToPointInTimeSettings,
  DefaultRetrySettings,
  DefaultRetryStrategy,
  DefaultUpdateTableSettings,
  GetItemSettings,
  PutItemSettings,
  RestoreTableToPointInTimeSettings,
  RetrySettings,
  RetryStrategy,
  UpdateItemSettings,
  UpdateTableSettings
}
import monix.reactive.{Consumer, Observable}
import monix.eval.Task
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{
  AttributeDefinition,
  AttributeValue,
  AutoScalingSettingsUpdate,
  BackupTypeFilter,
  BatchGetItemResponse,
  BatchWriteItemResponse,
  BillingMode,
  ContributorInsightsAction,
  CreateBackupResponse,
  CreateGlobalTableResponse,
  CreateTableResponse,
  DescribeBackupResponse,
  DynamoDbRequest,
  DynamoDbResponse,
  GetItemResponse,
  GlobalSecondaryIndex,
  GlobalSecondaryIndexAutoScalingUpdate,
  GlobalTableGlobalSecondaryIndexSettingsUpdate,
  KeySchemaElement,
  KeysAndAttributes,
  ListBackupsResponse,
  LocalSecondaryIndex,
  PointInTimeRecoverySpecification,
  ProvisionedThroughput,
  PutItemRequest,
  PutItemResponse,
  Replica,
  ReplicaAutoScalingUpdate,
  ReplicaSettingsUpdate,
  ReplicaUpdate,
  RestoreTableFromBackupResponse,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  ReturnValue,
  Tag,
  TransactGetItemsResponse,
  UntagResourceRequest,
  UntagResourceResponse,
  UpdateContinuousBackupsResponse,
  UpdateGlobalTableResponse,
  UpdateTableResponse,
  WriteRequest
}
import DynamoDbOp.Implicits._
import DynamoDbOp.Implicits.{createBackupOp, createGlobalTableOp, createTableOp, getItemOp, putItemOp}
import cats.effect.Resource
import com.amazonaws.regions.Regions
//import monix.connect.aws.auth.AppConf
import monix.execution.annotations.UnsafeBecauseImpure
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.regions.Region

import scala.concurrent.duration.FiniteDuration

/**
  * An idiomatic DynamoDb client integrated with Monix ecosystem.
  *
  * It is built on top of the [[DynamoDbAsyncClient]], reason why all the exposed methods
  * expect an implicit instance of the client to be in the scope of the call.
  */
object DynamoDb { self =>

 /* /**
    * Creates a [[Resource]] that will use the values from a
    * configuration file to allocate and release a [[DynamoDb]].
    * Thus, the api expects an `application.conf` file to be present
    * in the `resources` folder.
    *
    * @see how does the expected `.conf` file should look like
    *      https://github.com/monix/monix-connect/blob/master/aws-auth/src/main/resources/reference.conf`
    *
    * @see the cats effect resource data type: https://typelevel.org/cats-effect/datatypes/resource.html
    *
    * @return a [[Resource]] of [[Task]] that allocates and releases [[DynamoDb]].
    */
  def fromConfig: Resource[Task, DynamoDb] = {
    Resource.make {
      for {
        clientConf  <- Task.eval(AppConf.loadOrThrow)
        asyncClient <- Task.now(AsyncClientConversions.fromMonixAwsConf(clientConf.monixAws))
      } yield {
        self.createUnsafe(asyncClient)
      }
    } { _.close }
  }
*/

  /**
    * Creates a [[Resource]] that will use the passed
    * AWS configurations to allocate and release [[DynamoDb]].
    * Thus, the api expects an `application.conf` file to be present
    * in the `resources` folder.
    *
    * @see how does the expected `.conf` file should look like
    *      https://github.com/monix/monix-connect/blob/master/aws-auth/src/main/resources/reference.conf`
    *
    * @see the cats effect resource data type: https://typelevel.org/cats-effect/datatypes/resource.html
    *
    * ==Example==
    *
    * {{{
    *   import cats.effect.Resource
    *   import monix.eval.Task
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region
    *
    *   val defaultCredentials = DefaultCredentialsProvider.create()
    *   val s3Resource: Resource[Task, DynamoDb] = DynamoDb.create(defaultCredentials, Region.AWS_GLOBAL)
    * }}}
    *
    * @param credentialsProvider Strategy for loading credentials and authenticate to AWS S3
    * @param region An Amazon Web Services region that hosts a set of Amazon services.
    * @param endpoint The endpoint with which the SDK should communicate.
    * @param httpClient Sets the [[SdkAsyncHttpClient]] that the SDK service client will use to make HTTP calls.
    * @return a [[Resource]] of [[Task]] that allocates and releases [[S3]].
    **/
  def create(
    credentialsProvider: AwsCredentialsProvider,
    region: Region,
    endpoint: Option[String] = None,
    httpClient: Option[SdkAsyncHttpClient] = None): Resource[Task, DynamoDb] = {
    Resource.make {
      Task.eval {
        val asyncClient = AsyncClientConversions.from(credentialsProvider, region, endpoint, httpClient)
        createUnsafe(asyncClient)
      }
    } { _.close }
  }

  /**
    * Creates a instance of [[DynamoDb]] out of a [[DynamoDbAsyncClient]].
    *
    * It provides a fast forward access to the [[DynamoDb]] that avoids
    * dealing with [[Resource]].
    *
    * Unsafe because the state of the passed [[DynamoDbAsyncClient]] is not guaranteed,
    * it can either be malformed or closed, which would result in underlying failures.
    *
    * @see [[DynamoDb]] and [[DynamoDb.create]] for a pure usage of [[DynamoDb]].
    * They both will make sure that the s3 connection is created with the required
    * resources and guarantee that the client was not previously closed.
    *
    * ==Example==
    *
    * {{{
    *   import java.time.Duration
    *   import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    *   import software.amazon.awssdk.regions.Region.AWS_GLOBAL
    *   import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
    * }}}
    *
    * @param dynamoDbAsyncClient an instance of a [[S3AsyncClient]].
    * @return An instance of [[S3]]
    */
  @UnsafeBecauseImpure
  def createUnsafe(dynamoDbAsyncClient: DynamoDbAsyncClient): DynamoDb = {
    new DynamoDb {
      override val asyncClient: DynamoDbAsyncClient = dynamoDbAsyncClient
    }
  }

  // @deprecated
  // def consumer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
  //   retries: Int = 0,
  //   delayAfterFailure: Option[FiniteDuration] = None)(
  //   implicit
  //   dynamoDbOp: DynamoDbOp[In, Out],
  //   client: DynamoDbAsyncClient): Consumer[In, Unit] = DynamoDbSubscriber(retries, delayAfterFailure)

  /**
    * Transformer that executes any given [[DynamoDbRequest]] and transforms them to its subsequent [[DynamoDbResponse]] within [[Task]].
    * It also provides with the flexibility of retrying a failed execution with delay to recover from it.
    *
    * @param retries the number of times that an operation can be retried before actually returning a failed [[Task]].
    *        it must be higher or equal than 0.
    * @param delayAfterFailure delay after failure for the execution of a single [[DynamoDbOp]].
    * @param dynamoDbOp implicit [[DynamoDbOp]] that abstracts the execution of the specific operation.
    * @param client asynchronous DynamoDb client.
    * @tparam In input type parameter that must be a subtype os [[DynamoDbRequest]].
    * @tparam Out output type parameter that must be a subtype os [[DynamoDbRequest]].
    * @return DynamoDb operation transformer: `Observable[DynamoDbRequest] => Observable[DynamoDbRequest]`.
    */
  @deprecated
  def transformer[In <: DynamoDbRequest, Out <: DynamoDbResponse](
    retries: Int = 0,
    delayAfterFailure: Option[FiniteDuration] = None)(
    implicit
    dynamoDbOp: DynamoDbOp[In, Out],
    client: DynamoDbAsyncClient): Observable[In] => Observable[Task[Out]] = { inObservable: Observable[In] =>
    inObservable.map(request => DynamoDbOp.create(request, retries, delayAfterFailure))
  }

}

trait DynamoDb { self =>

  implicit val asyncClient: DynamoDbAsyncClient

  def batchGetItem(
    requestItems: Map[String, KeysAndAttributes],
    consumedCapacity: ReturnConsumedCapacity,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[BatchGetItemResponse] = {
    val batchGetItemRequest = RequestFactory.batchGetItem(requestItems, consumedCapacity)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(batchGetItemRequest, retries, Some(backoffDelay))
  }

  def batchWrite(
    requestItems: Map[String, List[WriteRequest]],
    returnConsumedCapacity: ReturnConsumedCapacity,
    returnItemCollectionMetrics: ReturnItemCollectionMetrics,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[BatchWriteItemResponse] = {
    val batchWriteRequest = RequestFactory.batchWrite(requestItems, returnConsumedCapacity, returnItemCollectionMetrics)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(batchWriteRequest, retries, Some(backoffDelay))
  }

  def createBackup(
    tableName: String,
    backupName: String,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[CreateBackupResponse] = {
    val createBackupRequest = RequestFactory.createBackup(backupName, tableName)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(createBackupRequest, retries, Some(backoffDelay))
  }

  //replicas is required
  def createGlobalTable(
    globalTableName: String,
    replicas: Seq[Replica],
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[CreateGlobalTableResponse] = {
    val createGlobalTableRequest = RequestFactory.createGlobalTable(globalTableName, replicas)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(createGlobalTableRequest, retries, Some(backoffDelay))
  }

  def createTable(
    tableName: String,
    keySchema: Seq[KeySchemaElement],
    attributeDefinitions: Seq[AttributeDefinition],
    createTableSettings: CreateTableSettings = DefaultCreateSettings,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[CreateTableResponse] = {
    val createTableRequest = RequestFactory.createTable(tableName, keySchema, attributeDefinitions, createTableSettings)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(createTableRequest, retries, Some(backoffDelay))
  }

  def deleteBackup(backupArn: String, retryStrategy: RetryStrategy = domain.DefaultRetryStrategy): Unit = {
    val deleteBackupRequest = RequestFactory.deleteBackup(backupArn)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(deleteBackupRequest, retries, Some(backoffDelay))
  }

  def describeBackup(
    backupArn: String,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[DescribeBackupResponse] = {
    val describeBackup = RequestFactory.describeBackup(backupArn)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(describeBackup, retries, Some(backoffDelay))
  }
  def describeContinuousBackups(tableName: String, retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val describeRequest = RequestFactory.describeContinuousBackups(tableName)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(describeRequest, retries, Some(backoffDelay))
  }
  def describeContributorInsights(
    tableName: String,
    indexName: String,
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val describeRequest = RequestFactory.describeContributorInsights(tableName, indexName)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(describeRequest, retries, Some(backoffDelay))
  }

  def describeEndpoints(retryStrategy: RetryStrategy = DefaultRetryStrategy): Unit = {
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(RequestFactory.describeEndpoints, retries, Some(backoffDelay))
  }

  def describeGlobalTable(globalTableName: String, retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val describeRequest = RequestFactory.describeGlobalTable(globalTableName)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(describeRequest, retries, Some(backoffDelay))
  }

  def describeLimits(retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(RequestFactory.describeLimits, retries, Some(backoffDelay))
  }
  def describeTableReplicaAutoScaling(tableName: String, retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val describeRequest = RequestFactory.describeTableReplicaAutoScaling(tableName)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(describeRequest, retries, Some(backoffDelay))
  }

  def deleteTable(tableName: String, retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val describeRequest = RequestFactory.deleteTable(tableName)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(describeRequest, retries, Some(backoffDelay))
  }

  def describeTimeToLive(tableName: String, retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val describeRequest = RequestFactory.describeTimeToLive(tableName)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(describeRequest, retries, Some(backoffDelay))
  }

  def getItem(
    key: Map[String, AttributeValue],
    tableName: String,
    projectionExpression: Option[String] = None,
    getItemSettings: GetItemSettings = domain.DefaultGetItemSettings,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[GetItemResponse] = {
    val getItemRequest =
      RequestFactory
        .getItem(key, tableName, projectionExpression, getItemSettings)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(getItemRequest, retries, Some(backoffDelay))
  }

  def listBackups(
    backupType: BackupTypeFilter,
    tableName: Option[String],
    exclusiveStartBackupArn: Option[String],
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[ListBackupsResponse] = {
    val listBackupsRequest = RequestFactory.listBackups(backupType, tableName, exclusiveStartBackupArn)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(listBackupsRequest, retries, Some(backoffDelay))
  }

  def listContributorInsights(
    tableName: String,
    maxResults: Int,
    nextToken: Option[String],
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val listRequest = RequestFactory.listContributorInsights(tableName, maxResults, nextToken)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(listRequest, retries, Some(backoffDelay))
  }

  def listTables(
    tablePrefix: Option[String] = None,
    limit: Int = 100,
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val listRequest = RequestFactory.listTables(tablePrefix, limit)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(listRequest, retries, Some(backoffDelay))
  }

  def listGlobalTables(
    tablePrefix: Option[String],
    limit: Int,
    region: Option[Regions],
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val listRequest = RequestFactory.listGlobalTables(tablePrefix, limit, region)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(listRequest, retries, Some(backoffDelay))
  }

  def listTagsOfResource(
    resourceArn: String,
    nextToken: Option[String] = None,
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val listRequest = RequestFactory.listTagsOfResource(resourceArn, nextToken)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(listRequest, retries, Some(backoffDelay))
  }

  def putItem(
    tableName: String,
    item: Map[String, AttributeValue],
    putItemSettings: PutItemSettings = DefaultPutItemSettings,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[PutItemResponse] = {
    val putRequest = RequestFactory.putItemRequest(tableName, item, putItemSettings)
    val RetryStrategy(retries, backoffDelay) = retryStrategy

    create(putRequest, retryStrategy.retries, Some(backoffDelay))
  }

  def putItemSink(
    retries: Int = 0,
    delayAfterFailure: Option[FiniteDuration] = None): Consumer[PutItemRequest, Unit] = {
    DynamoDbSubscriber(retries, delayAfterFailure, self)
  }

  def restoreTableFromBackup(
    backupArn: String,
    targetTableName: String,
    settings: domain.RestoreTableFromBackupSettings = DefaultRestoreTableFromBackupSettings,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[RestoreTableFromBackupResponse] = {
    val restoreTableRequest = RequestFactory.restoreTableFromBackup(backupArn, targetTableName, settings)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(restoreTableRequest, retries, Some(backoffDelay))
  }

  def restoreTableToPointInTime(
    backupArn: String,
    targetTableName: String,
    sourceTableName: Option[String],
    restoreSettings: RestoreTableToPointInTimeSettings = DefaultRestoreTableToPointInTimeSettings,
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val restoreTableRequest =
      RequestFactory.restoreTableToPointInTime(backupArn, targetTableName, sourceTableName, restoreSettings)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(restoreTableRequest, retries, Some(backoffDelay))
  }

  def tagResource(resourceArn: String, tags: Seq[Tag], retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val tagResourceRequest = RequestFactory.tagResource(resourceArn, tags: _*)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(tagResourceRequest, retries, Some(backoffDelay))
  }

  //only supports one item
  def transactGetItem(
    tableName: String,
    key: Map[String, AttributeValue],
    projectionExpression: Option[String],
    expressionAttributeNames: Map[String, String],
    returnConsumedCapacity: ReturnConsumedCapacity,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[TransactGetItemsResponse] = {
    val transactGetItemRequest = RequestFactory
      .transactGetItem(tableName, key, projectionExpression, expressionAttributeNames, returnConsumedCapacity)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(transactGetItemRequest, retries, Some(backoffDelay))
  }

  //not yet supported
  //def transactWriteItems(retryStrategy: RetryStrategy = DefaultRetryStrategy) =
  def untagResource(
    resourceArn: String,
    tagKeys: Seq[String],
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UntagResourceResponse] = {
    val untagRequest = RequestFactory.untagResource(resourceArn: String, tagKeys: _*)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(untagRequest, retries, Some(backoffDelay))
  }

  def updateContinuousBackups(
    tableName: String,
    pointInTimeRecoverySpecification: PointInTimeRecoverySpecification,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateContinuousBackupsResponse] = {
    val updateBackupsRequest = RequestFactory.updateContinuousBackups(tableName, pointInTimeRecoverySpecification)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(updateBackupsRequest, retries, Some(backoffDelay))
  }

  def updateContributorInsights(
    tableName: String,
    contributorInsightsAction: ContributorInsightsAction,
    indexName: Option[String],
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val updateRequest = RequestFactory.updateContributorInsights(tableName, contributorInsightsAction, indexName)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(updateRequest, retries, Some(backoffDelay))
  }

  def updateGlobalTable(
    globalTableName: String,
    replicaUpdates: Seq[ReplicaUpdate],
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateGlobalTableResponse] = {
    val updateRequest = RequestFactory.updateGlobalTable(globalTableName, replicaUpdates: _*)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(updateRequest, retries, Some(backoffDelay))
  }

  def updateGlobalTableSettings(
    globalTableName: String,
    secondaryIndexSettings: Seq[GlobalTableGlobalSecondaryIndexSettingsUpdate],
    autoScalingSettingsUpdate: Option[AutoScalingSettingsUpdate],
    provisionedWriteCapacityUnits: Option[Long],
    billingMode: BillingMode,
    replicaSettingsUpdate: Seq[ReplicaSettingsUpdate],
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val updateRequest = RequestFactory.updateGlobalTableSettings(
      globalTableName,
      secondaryIndexSettings,
      autoScalingSettingsUpdate,
      provisionedWriteCapacityUnits,
      billingMode,
      replicaSettingsUpdate)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(updateRequest, retries, Some(backoffDelay))
  }

  def updateItem(
    tableName: String,
    key: String,
    updateExpression: Option[String],
    conditionExpression: Option[String],
    updateItemSettings: UpdateItemSettings,
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val updateItemRequest =
      RequestFactory.updateItem(tableName, key, updateExpression, conditionExpression, updateItemSettings)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(updateItemRequest, retries, Some(backoffDelay))
  }

  def updateTableReplicaAutoScaling(
    tableName: String,
    globalSecondaryIndex: Seq[GlobalSecondaryIndexAutoScalingUpdate],
    autoScalingSettingsUpdate: Option[AutoScalingSettingsUpdate],
    replicaAutoScalingUpdates: Seq[ReplicaAutoScalingUpdate],
    retryStrategy: RetryStrategy = DefaultRetryStrategy) = {
    val updateRequest = RequestFactory.updateTableReplicaAutoScaling(
      tableName,
      globalSecondaryIndex,
      autoScalingSettingsUpdate,
      replicaAutoScalingUpdates)
    val RetryStrategy(retries, backoffDelay) = retryStrategy
    create(updateRequest, retries, Some(backoffDelay))
  }

  //def updateTable(
  //  tableName: String,
  //  attributeDefinitions: Seq[AttributeDefinition],
  //  updateTableSettings: UpdateTableSettings = DefaultUpdateTableSettings,
  //  retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateTableResponse] = {
  //  val updateTableRequest = RequestFactory.updateTable(tableName, attributeDefinitions, updateTableSettings)
  //  val RetryStrategy(retries, backoffDelay) = retryStrategy
  //  create(updateTableRequest, retries, Some(backoffDelay))
  //}

  val close: Task[Unit] = Task(asyncClient.close())
}
