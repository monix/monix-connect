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

package monix.connect.mongodb

import com.mongodb.client.model.{
  CountOptions,
  DeleteOptions,
  FindOneAndDeleteOptions,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions,
  InsertManyOptions,
  InsertOneOptions,
  ReplaceOptions,
  UpdateOptions
}
import monix.execution.internal.InternalApi

import scala.concurrent.duration.{Duration, FiniteDuration}

package object domain {

  //default option instances
  @InternalApi private[mongodb] val DefaultDeleteOptions = new DeleteOptions()
  @InternalApi private[mongodb] val DefaultCountOptions = new CountOptions()
  @InternalApi private[mongodb] val DefaultFindOneAndDeleteOptions = new FindOneAndDeleteOptions()
  @InternalApi private[mongodb] val DefaultFindOneAndReplaceOptions = new FindOneAndReplaceOptions()
  @InternalApi private[mongodb] val DefaultFindOneAndUpdateOptions = new FindOneAndUpdateOptions()
  @InternalApi private[mongodb] val DefaultInsertOneOptions = new InsertOneOptions()
  @InternalApi private[mongodb] val DefaultInsertManyOptions = new InsertManyOptions()
  @InternalApi private[mongodb] val DefaultUpdateOptions = new UpdateOptions()
  @InternalApi private[mongodb] val DefaultReplaceOptions = new ReplaceOptions()

  //results
  case class DeleteResult(deleteCount: Long, wasAcknowledged: Boolean)
  case class InsertOneResult(insertedId: Option[String], wasAcknowledged: Boolean)
  case class InsertManyResult(insertedIds: Set[String], wasAcknowledged: Boolean)
  case class UpdateResult(matchedCount: Long, modifiedCount: Long, wasAcknowledged: Boolean)

  //default result instances
  @InternalApi private[mongodb] val DefaultDeleteResult = DeleteResult(deleteCount = 0L, wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultInsertOneResult =
    InsertOneResult(insertedId = Option.empty[String], wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultInsertManyResult =
    InsertManyResult(insertedIds = Set.empty[String], wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultUpdateResult =
    UpdateResult(matchedCount = 0L, modifiedCount = 0L, wasAcknowledged = false)

  /**
    * A retry strategy is defined by the amount of retries and backoff delay per operation.
    *
    * @param attempts the number of times that an operation can be
    *                 retried before actually returning a failed task.
    *                 it must be higher or equal than 1.
    * @param backoffDelay delay after failure for the execution of a single mongodb operation.
    */
  case class RetryStrategy(attempts: Int = 0, backoffDelay: FiniteDuration = Duration.Zero)
  final val DefaultRetryStrategy = RetryStrategy(1, Duration.Zero)

  type Tuple2F[T[_], A, B] = (T[A], T[B])
  type Tuple3F[T[_], A, B, C] = (T[A], T[B], T[C])
  type Tuple4F[T[_], A, B, C, D] = (T[A], T[B], T[C], T[D])
  type Tuple5F[T[_], A, B, C, D, E] = (T[A], T[B], T[C], T[D], T[E])
  type Tuple6F[T[_], A, B, C, D, E, F] = (T[A], T[B], T[C], T[D], T[E], T[F])
  type Tuple7F[T[_], A, B, C, D, E, F, G] = (T[A], T[B], T[C], T[D], T[E], T[F], T[G])
  type Tuple8F[T[_], A, B, C, D, E, F, G, H] = (T[A], T[B], T[C], T[D], T[E], T[F], T[G], T[H])

}
