package monix.connect.dynamodb

import java.time.Instant

import com.amazonaws.regions.{Region, Regions}
import monix.connect.dynamodb.domain.{CreateTableSettings, GetItemSettings, ListBackupsSettings, RetrySettings, UpdateItemSettings, UpdateTableSettings}
import software.amazon.awssdk.services.dynamodb.model.{AttributeDefinition, AttributeValue, AttributeValueUpdate, AutoScalingSettingsUpdate, BackupType, BackupTypeFilter, BatchGetItemRequest, BatchWriteItemRequest, BillingMode, ContributorInsightsAction, CreateBackupRequest, CreateGlobalTableRequest, CreateTableRequest, DeleteBackupRequest, DeleteTableRequest, DescribeBackupRequest, DescribeContinuousBackupsRequest, DescribeContributorInsightsRequest, DescribeEndpointsRequest, DescribeGlobalTableRequest, DescribeLimitsRequest, DescribeTableReplicaAutoScalingRequest, DescribeTimeToLiveRequest, Get, GetItemRequest, GlobalSecondaryIndex, GlobalSecondaryIndexAutoScalingUpdate, GlobalSecondaryIndexUpdate, GlobalTableGlobalSecondaryIndexSettingsUpdate, KeySchemaElement, KeysAndAttributes, ListBackupsRequest, ListContributorInsightsRequest, ListGlobalTablesRequest, ListTablesRequest, ListTagsOfResourceRequest, LocalSecondaryIndex, PointInTimeRecoverySpecification, ProvisionedThroughput, PutItemRequest, Replica, ReplicaAutoScalingUpdate, ReplicaSettingsUpdate, ReplicaUpdate, ReplicationGroupUpdate, RestoreTableFromBackupRequest, RestoreTableToPointInTimeRequest, ReturnConsumedCapacity, ReturnItemCollectionMetrics, ReturnValue, SSESpecification, StreamSpecification, Tag, TagResourceRequest, TransactGetItem, TransactGetItemsRequest, TransactWriteItem, TransactWriteItemsRequest, UntagResourceRequest, UpdateContinuousBackupsRequest, UpdateContributorInsightsRequest, UpdateGlobalTableRequest, UpdateGlobalTableSettingsRequest, UpdateItemRequest, UpdateTableReplicaAutoScalingRequest, UpdateTableRequest}
import sun.tools.tree.ConditionalExpression

import scala.jdk.CollectionConverters._

object RequestFactory {

  def batchGetItem(
    requestItems: Map[String, KeysAndAttributes],
    consumedCapacity: ReturnConsumedCapacity): BatchGetItemRequest =
    BatchGetItemRequest.builder().requestItems(requestItems.asJava).returnConsumedCapacity(consumedCapacity).build()

  def batchWrite(
    requestItems: Map[String, KeysAndAttributes],
    returnConsumedCapacity: ReturnConsumedCapacity,
    returnItemCollectionMetrics: ReturnItemCollectionMetrics): BatchWriteItemRequest = {
    BatchWriteItemRequest.builder
      .requestItems(requestItems.asJava)
      .returnConsumedCapacity(returnConsumedCapacity)
      .returnItemCollectionMetrics(returnItemCollectionMetrics)
      .build()
  }

  def createBackup(tableName: String, backupName: String): CreateBackupRequest = {
    CreateBackupRequest.builder().backupName(backupName).tableName(tableName).build
  }

  def createGlobalTable(globalTableName: String, replicas: Seq[Replica]): CreateGlobalTableRequest = {
    CreateGlobalTableRequest.builder().globalTableName(globalTableName).replicationGroup(replicas: _*).build
  }

  def createTable(
    tableName: String,
    keySchema: Seq[KeySchemaElement],
    attributeDefinitions: Seq[AttributeDefinition],
    createTableSettings: CreateTableSettings): CreateTableRequest = {
    val builder =
      CreateTableRequest
        .builder()
        .tableName(tableName)
        .keySchema(keySchema: _*)
        .attributeDefinitions(attributeDefinitions: _*)
        .billingMode(createTableSettings.billingMode)
        .globalSecondaryIndexes(createTableSettings.globalSecondaryIndex: _*)
        .localSecondaryIndexes(createTableSettings.localSecondaryIndexes: _*)
        .tags(createTableSettings.tags: _*)
    createTableSettings.provisionedThroughput.map(builder.provisionedThroughput)
    createTableSettings.sseSpecification.map(builder.sseSpecification)
    builder.build
  }

  def deleteBackup(backupArn: String): DeleteBackupRequest =
    DeleteBackupRequest.builder.backupArn(backupArn).build

  def describeBackup(backupArn: String): DescribeBackupRequest =
    DescribeBackupRequest.builder.backupArn(backupArn).build

  def describeContinuousBackups(tableName: String): DescribeContinuousBackupsRequest =
    DescribeContinuousBackupsRequest.builder.tableName(tableName).build

  def describeContributorInsights(tableName: String, indexName: String): DescribeContributorInsightsRequest =
    DescribeContributorInsightsRequest.builder.tableName(tableName).indexName(indexName).build

  def describeEndpoints: DescribeEndpointsRequest = {
    DescribeEndpointsRequest.builder.build
  }

  def describeGlobalTable(globalTableName: String): DescribeGlobalTableRequest =
    DescribeGlobalTableRequest.builder.globalTableName(globalTableName).build

  def describeLimits: DescribeLimitsRequest = {
    DescribeLimitsRequest.builder.build
  }

  def describeTableReplicaAutoScaling(tableName: String): DescribeTableReplicaAutoScalingRequest =
    DescribeTableReplicaAutoScalingRequest.builder().tableName(tableName).build

  def deleteTable(tableName: String): DeleteTableRequest =
    DeleteTableRequest.builder().tableName(tableName).build

  def describeTimeToLive(tableName: String): DescribeTimeToLiveRequest =
    DescribeTimeToLiveRequest.builder().tableName(tableName).build

  def getItem(
    key: Map[String, AttributeValue],
    tableName: String,
    projectionExpression: Option[String],
    getItemSettings: GetItemSettings): GetItemRequest = {
    val builder = GetItemRequest
      .builder()
      .tableName(tableName)
      .key(key.asJava)
      .consistentRead(getItemSettings.consistentRead)
      .expressionAttributeNames(getItemSettings.expressionAttributeNames.asJava)
      .returnConsumedCapacity(getItemSettings.returnConsumedCapacity.toString)
    projectionExpression.map(builder.projectionExpression)
    builder.build()
  }

  def listBackups(
    backupType: BackupTypeFilter,
    tableName: Option[String],
    exclusiveStartBackupArn: Option[String],
    listBackupSettings: ListBackupsSettings = domain.DefaultListBackupsSettings): ListBackupsRequest = {
    val listBackupsRequest = ListBackupsRequest
      .builder()
      .backupType(backupType)
      .timeRangeLowerBound(listBackupSettings.timeRangeLowerBound)
      .timeRangeUpperBound(listBackupSettings.timeRangeUppeBound)
      .limit(listBackupSettings.limit)
    exclusiveStartBackupArn.map(listBackupsRequest.exclusiveStartBackupArn)
    tableName.map(listBackupsRequest.tableName)
    listBackupsRequest.build
  }

  def listContributorInsights(
    tableName: String,
    maxResults: Int,
    nextToken: Option[String]): ListContributorInsightsRequest = {
    val listRequest = ListContributorInsightsRequest.builder().tableName(tableName).maxResults(maxResults)
    nextToken.map(listRequest.nextToken)
    listRequest.build
  }

  def listTables(exclusiveStartTableName: Option[String], limit: Int): ListTablesRequest = {
    val listRequest = ListTablesRequest.builder().limit(limit)
    exclusiveStartTableName.map(listRequest.exclusiveStartTableName)
    listRequest.build
  }

  def listGlobalTables(regionName: Regions, limit: Int, startTableName: Option[String]): ListGlobalTablesRequest = {
    val listRequest = ListGlobalTablesRequest.builder().regionName(regionName.getName).limit(limit)
    startTableName.map(listRequest.exclusiveStartGlobalTableName)
    listRequest.build
  }

  def listTagsOfResource(resourceArn: String, nextToken: Option[String]) = {
    val listRequest = ListTagsOfResourceRequest.builder.resourceArn(resourceArn)
    nextToken.map(listRequest.nextToken)
    listRequest.build
  }

  def putItemRequest(tableName: String, item: Map[String, AttributeValue], putItemSettings: domain.PutItemSettings) = {
    // expected and conditional operator are legacy therefore were not included
    val builder = PutItemRequest.builder
      .tableName(tableName)
      .item(item.asJava)
      .expressionAttributeNames(putItemSettings.expressionAttributeNames.asJava)
      .expressionAttributeValues(putItemSettings.expressionAttributeValues.asJava)
      .returnConsumedCapacity(putItemSettings.consumedCapacityDetails)
      .returnItemCollectionMetrics(putItemSettings.returnItemCollectionMetrics)
      .returnValues(putItemSettings.returnValue)
    putItemSettings.conditionalExpression.map(builder.conditionExpression)
    builder.build
  }

  def restoreTableFromBackup(
    backupArn: String,
    targetTableName: String,
    settings: domain.RestoreTableFromBackupSettings): RestoreTableFromBackupRequest = {
    val restoreRequest = RestoreTableFromBackupRequest
      .builder
      .backupArn(backupArn)
      .targetTableName(targetTableName)
      .billingModeOverride(settings.billingMode)
      .globalSecondaryIndexOverride(settings.globalSecondaryIndex: _*)
      .localSecondaryIndexOverride(settings.localSecondaryIndex: _*)
    settings.provisionedThroughput.map(restoreRequest.provisionedThroughputOverride)
    restoreRequest.build
  }

  def restoreTableToPointInTime(
    backupArn: String,
    targetTableName: String,
    sourceTableName: Option[String],
    settings: domain.RestoreTableToPointInTimeSettings = domain.DefaultRestoreTableToPointInTimeSettings)
    : RestoreTableToPointInTimeRequest = {
    val restoreRequest = RestoreTableToPointInTimeRequest.builder
      .targetTableName(targetTableName)
      .billingModeOverride(settings.billingMode)
      .globalSecondaryIndexOverride(settings.globalSecondaryIndex: _*)
      .localSecondaryIndexOverride(settings.localSecondaryIndex: _*)
    settings.restoreDateTime match {
      case Some(instant) => restoreRequest.restoreDateTime(instant)
      case None => restoreRequest.useLatestRestorableTime(true)
    }
    sourceTableName.map(restoreRequest.sourceTableName)
    settings.provisionedThroughput.map(restoreRequest.provisionedThroughputOverride)
    restoreRequest.build
  }

  def tagResource(resourceArn: String, tags: Tag*): TagResourceRequest =
    TagResourceRequest.builder.resourceArn(resourceArn).tags(tags: _*).build

  def transactGetItem(
    tableName: String,
    key: Map[String, AttributeValue],
    projectionExpression: Option[String],
    expressionAttributeNames: Map[String, String]): TransactGetItem = {
    val get = Get
      .builder()
      .tableName(tableName)
      .key(key.asJava)
      .expressionAttributeNames(expressionAttributeNames.asJava)
    projectionExpression.map(get.projectionExpression)
    TransactGetItem.builder().get(get.build).build
  }

  def transactGetItem(tableName: String,
                       key: Map[String, AttributeValue],
                      projectionExpression: Option[String],
                      expressionAttributeNames: Map[String, String],
                      returnConsumedCapacity: ReturnConsumedCapacity): TransactGetItemsRequest = {
    val transactGetItem = RequestFactory.transactGetItem(tableName,
      key,
      projectionExpression,
      expressionAttributeNames)
    TransactGetItemsRequest.builder().transactItems(Seq(transactGetItem): _*).returnConsumedCapacity(returnConsumedCapacity).build()
  }


  //todo
  def transactWriteItems(transactItems: TransactWriteItem): TransactWriteItem.Builder = {
    ???
  }

  def untagResource(resourceArn: String, tagKeys: String*): UntagResourceRequest =
    UntagResourceRequest.builder().resourceArn(resourceArn).tagKeys(tagKeys: _*).build

  def updateContinuousBackups(
    tableName: String,
    pointInTimeRecoverySpecification: PointInTimeRecoverySpecification): UpdateContinuousBackupsRequest =
    UpdateContinuousBackupsRequest
      .builder()
      .tableName(tableName)
      .pointInTimeRecoverySpecification(pointInTimeRecoverySpecification)
      .build

  def updateContributorInsights(
    tableName: String,
    contributorInsightsAction: ContributorInsightsAction,
    indexName: Option[String]): UpdateContributorInsightsRequest = {
    val updateRequest = UpdateContributorInsightsRequest
      .builder()
      .tableName(tableName)
      .contributorInsightsAction(contributorInsightsAction)
    indexName.map(updateRequest.indexName)
    updateRequest.build
  }

  def updateGlobalTable(globalTableName: String, replicaUpdates: ReplicaUpdate*): UpdateGlobalTableRequest = {
    UpdateGlobalTableRequest.builder().globalTableName(globalTableName).replicaUpdates(replicaUpdates: _*).build
  }

  def updateGlobalTableSettings(globalTableName: String,
                                secondaryIndexSettings: Seq[GlobalTableGlobalSecondaryIndexSettingsUpdate],
                                autoScalingSettingsUpdate: Option[AutoScalingSettingsUpdate],
                                provisionedWriteCapacityUnits: Option[Long],
                                billingMode: BillingMode,
                                replicaSettingsUpdate: Seq[ReplicaSettingsUpdate]): UpdateGlobalTableSettingsRequest = {
    val updateRequest = UpdateGlobalTableSettingsRequest.builder
      .globalTableName(globalTableName).globalTableBillingMode(billingMode)
      .globalTableGlobalSecondaryIndexSettingsUpdate(secondaryIndexSettings: _*)
      .replicaSettingsUpdate(replicaSettingsUpdate: _*)
    provisionedWriteCapacityUnits.map(updateRequest.globalTableProvisionedWriteCapacityUnits(_))
    autoScalingSettingsUpdate.map(updateRequest.globalTableProvisionedWriteCapacityAutoScalingSettingsUpdate)
    updateRequest.build()
  }

  def updateItem(
    tableName: String,
    key: String,
    updateExpression: Option[String],
    conditionExpression: Option[String],
    updateItemSettings: UpdateItemSettings): UpdateItemRequest = {
    val updateRequest = UpdateItemRequest
      .builder
      .tableName(tableName)
      .expressionAttributeNames(updateItemSettings.expressionAttributeNames.asJava)
      .expressionAttributeValues(updateItemSettings.expressionAttributeValues.asJava)
      .returnConsumedCapacity(updateItemSettings.consumedCapacity)
      .returnItemCollectionMetrics(updateItemSettings.returnItemCollectionMetrics)
      .returnValues(updateItemSettings.returnValue)
    updateExpression.map(updateRequest.updateExpression)
    conditionExpression.map(updateRequest.conditionExpression)
    updateRequest.build
  }

  def updateTableReplicaAutoScaling(
    tableName: String,
    globalSecondaryIndex: Seq[GlobalSecondaryIndexAutoScalingUpdate],
    autoScalingSettingsUpdate: Option[AutoScalingSettingsUpdate],
    replicaAutoScalingUpdates: Seq[ReplicaAutoScalingUpdate]): UpdateTableReplicaAutoScalingRequest = {
    val updateRequest = UpdateTableReplicaAutoScalingRequest.builder
      .globalSecondaryIndexUpdates(globalSecondaryIndex: _*)
      .replicaUpdates(replicaAutoScalingUpdates: _*)
    autoScalingSettingsUpdate.map(updateRequest.provisionedWriteCapacityAutoScalingUpdate)
    updateRequest.build
  }

  def updateTable(
    tableName: String,
    attributeDefinitions: Seq[AttributeDefinition],
    updateTableSettings: UpdateTableSettings): UpdateTableRequest = {

    val updateRequest = UpdateTableRequest.builder
      .tableName(tableName)
      .attributeDefinitions(attributeDefinitions: _*)
      .billingMode(billingMode)
      .globalSecondaryIndexUpdates(globalSecondaryIndex: _*)
      .replicaUpdates(replicaUpdates: _*)
    ssESpecification.map(updateRequest.sseSpecification)
    provisionedThroughput.map(updateRequest.provisionedThroughput)
    streamSpecification.map(updateRequest.streamSpecification)
    updateRequest.build
  }

}
