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

import java.time.Instant

import software.amazon.awssdk.services.dynamodb.model.{
  AttributeValue,
  BillingMode,
  GlobalSecondaryIndex,
  GlobalSecondaryIndexUpdate,
  LocalSecondaryIndex,
  ProvisionedThroughput,
  ReplicationGroupUpdate,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  ReturnValue,
  SSESpecification,
  StreamSpecification,
  Tag
}

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

package object domain {

  @deprecated("See `RetryStrategy`")
  case class RetrySettings(retries: Int, delayAfterFailure: Option[FiniteDuration])
  final val DefaultRetrySettings = RetrySettings(0, Option.empty)

  case class RetryStrategy(retries: Int, backoffDelay: FiniteDuration)
  final val DefaultRetryStrategy = RetryStrategy(0, 0.seconds)

  case class PutItemSettings(
    conditionalExpression: Option[String],
    expressionAttributeNames: Map[String, String],
    expressionAttributeValues: Map[String, AttributeValue],
    consumedCapacityDetails: ReturnConsumedCapacity,
    returnItemCollectionMetrics: ReturnItemCollectionMetrics,
    returnValue: ReturnValue)

  final val DefaultPutItemSettings = PutItemSettings(
    conditionalExpression = Option.empty,
    expressionAttributeNames = Map.empty,
    expressionAttributeValues = Map.empty,
    consumedCapacityDetails = ReturnConsumedCapacity.NONE,
    returnItemCollectionMetrics = ReturnItemCollectionMetrics.NONE,
    returnValue = ReturnValue.NONE
  )

  case class CreateTableSettings(
    billingMode: BillingMode,
    globalSecondaryIndex: Seq[GlobalSecondaryIndex],
    localSecondaryIndexes: Seq[LocalSecondaryIndex],
    tags: Seq[Tag],
    provisionedThroughput: ProvisionedThroughput,
    sseSpecification: Option[SSESpecification])

  final val DefaultCreateSettings = CreateTableSettings(
    billingMode = BillingMode.PAY_PER_REQUEST,
    globalSecondaryIndex = Seq.empty[GlobalSecondaryIndex],
    localSecondaryIndexes = Seq.empty[LocalSecondaryIndex],
    provisionedThroughput = ProvisionedThroughput.builder.build,
    sseSpecification = None,
    tags = Seq.empty
  )

  case class GetItemSettings(
    expressionAttributeNames: Map[String, String],
    consistentRead: Boolean,
    returnConsumedCapacity: ReturnConsumedCapacity)

  final val DefaultGetItemSettings = GetItemSettings(
    expressionAttributeNames = Map.empty,
    consistentRead = true,
    returnConsumedCapacity = ReturnConsumedCapacity.NONE)

  case class ListBackupsSettings(
    exclusiveStartBackupArn: Option[String],
    timeRangeLowerBound: Instant,
    timeRangeUppeBound: Instant,
    limit: Int)

  final val DefaultListBackupsSettings = ListBackupsSettings(
    exclusiveStartBackupArn = None,
    timeRangeLowerBound = Instant.MIN,
    timeRangeUppeBound = Instant.MAX,
    limit = 1)

  case class RestoreTableToPointInTimeSettings(
    restoreDateTime: Option[Instant],
    billingMode: BillingMode,
    globalSecondaryIndex: Seq[GlobalSecondaryIndex],
    localSecondaryIndex: Seq[LocalSecondaryIndex],
    provisionedThroughput: Option[ProvisionedThroughput])

  final val DefaultRestoreTableToPointInTimeSettings =
    RestoreTableToPointInTimeSettings(None, BillingMode.UNKNOWN_TO_SDK_VERSION, Seq.empty, Seq.empty, None)

  case class RestoreTableFromBackupSettings(
    billingMode: BillingMode,
    globalSecondaryIndex: Seq[GlobalSecondaryIndex],
    localSecondaryIndex: Seq[LocalSecondaryIndex],
    provisionedThroughput: Option[ProvisionedThroughput])

  final val DefaultRestoreTableFromBackupSettings = RestoreTableFromBackupSettings(
    billingMode = BillingMode.UNKNOWN_TO_SDK_VERSION,
    globalSecondaryIndex = Seq.empty,
    localSecondaryIndex = Seq.empty,
    provisionedThroughput = None
  )

  case class UpdateItemSettings(
    expressionAttributeNames: Map[String, String],
    expressionAttributeValues: Map[String, AttributeValue],
    consumedCapacity: ReturnConsumedCapacity,
    returnItemCollectionMetrics: ReturnItemCollectionMetrics,
    returnValue: ReturnValue)

  final val DefaultUpdateItemSetting = UpdateItemSettings(
    expressionAttributeNames = Map.empty,
    expressionAttributeValues = Map.empty,
    consumedCapacity = ReturnConsumedCapacity.UNKNOWN_TO_SDK_VERSION,
    returnItemCollectionMetrics = ReturnItemCollectionMetrics.UNKNOWN_TO_SDK_VERSION,
    returnValue = ReturnValue.UNKNOWN_TO_SDK_VERSION
  )

  case class UpdateTableSettings(
    billingMode: BillingMode,
    globalSecondaryIndex: Seq[GlobalSecondaryIndexUpdate],
    replicaUpdates: Seq[ReplicationGroupUpdate],
    provisionedThroughput: Option[ProvisionedThroughput],
    ssESpecification: Option[SSESpecification],
    streamSpecification: Option[StreamSpecification])

  final val DefaultUpdateTableSettings = UpdateTableSettings(
    billingMode = BillingMode.UNKNOWN_TO_SDK_VERSION,
    globalSecondaryIndex = Seq.empty,
    replicaUpdates = Seq.empty,
    provisionedThroughput = Option.empty,
    ssESpecification = Option.empty,
    streamSpecification = Option.empty
  )
}
