/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

package monix.connect.mongodb

import monix.execution.internal.InternalApi

import scala.util.Try
import scala.jdk.CollectionConverters._

/** Contains the methods for converting unsafe java mongodb results to scala safe ones. */
@InternalApi private[mongodb] object ResultConverter {

  def fromJava(deleteResult: com.mongodb.client.result.DeleteResult): DeleteResult = {
    val deleteCount: Long = Try(deleteResult.getDeletedCount()).getOrElse(0)
    val wasAcknowledged = Try(deleteResult.wasAcknowledged()).getOrElse(false)
    DeleteResult(deleteCount, wasAcknowledged)
  }

  def fromJava(insertOneResult: com.mongodb.client.result.InsertOneResult): InsertOneResult = {
    val insertedId = Some(insertOneResult.getInsertedId.asObjectId.getValue.toString)
    val wasAcknowledged = Try(insertOneResult.wasAcknowledged).getOrElse(false)
    InsertOneResult(insertedId, wasAcknowledged)
  }

  def fromJava(insertManyResult: com.mongodb.client.result.InsertManyResult): InsertManyResult = {
    val insertedIds: Set[String] = Try {
      insertManyResult.getInsertedIds
        .entrySet()
        .asScala
        .map(_.getValue.asObjectId.getValue.toString)
        .toSet[String]
    }.getOrElse(Set.empty)
    val wasAcknowledged = Try(insertManyResult.wasAcknowledged).getOrElse(false)
    InsertManyResult(insertedIds, wasAcknowledged)
  }

  def fromJava(updateResult: com.mongodb.client.result.UpdateResult): UpdateResult = {
    /* the upsertedId is not being mapped since it is an optional field
     * that the user knows when performing the operation */
    val matchedCount: Long = Try(updateResult.getMatchedCount).getOrElse(0)
    val wasAcknowledged: Boolean = Try(updateResult.wasAcknowledged).getOrElse(false)
    val modifiedCount: Long = Try(updateResult.getModifiedCount).getOrElse(0)
    UpdateResult(matchedCount, modifiedCount, wasAcknowledged)
  }

}
