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
  CreateIndexOptions,
  DeleteOptions,
  FindOneAndDeleteOptions,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions,
  IndexOptions,
  InsertManyOptions,
  InsertOneOptions,
  ReplaceOptions,
  UpdateOptions
}
import monix.execution.internal.InternalApi

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
  @InternalApi private[mongodb] val DefaultIndexOptions = new IndexOptions()
  @InternalApi private[mongodb] val DefaultCreateIndexesOptions = new CreateIndexOptions()

  //default result instances
  @InternalApi private[mongodb] val DefaultDeleteResult = DeleteResult(deleteCount = 0L, wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultInsertOneResult =
    InsertOneResult(insertedId = Option.empty[String], wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultInsertManyResult =
    InsertManyResult(insertedIds = Set.empty[String], wasAcknowledged = false)
  @InternalApi private[mongodb] val DefaultUpdateResult =
    UpdateResult(matchedCount = 0L, modifiedCount = 0L, wasAcknowledged = false)

  //default strategy instance
  final val DefaultRetryStrategy = RetryStrategy()

  type Tuple2F[T[_], A, B] = (T[A], T[B])
  type Tuple3F[T[_], A, B, C] = (T[A], T[B], T[C])
  type Tuple4F[T[_], A, B, C, D] = (T[A], T[B], T[C], T[D])
  type Tuple5F[T[_], A, B, C, D, E] = (T[A], T[B], T[C], T[D], T[E])
  type Tuple6F[T[_], A, B, C, D, E, F] = (T[A], T[B], T[C], T[D], T[E], T[F])
  type Tuple7F[T[_], A, B, C, D, E, F, G] = (T[A], T[B], T[C], T[D], T[E], T[F], T[G])
  type Tuple8F[T[_], A, B, C, D, E, F, G, H] = (T[A], T[B], T[C], T[D], T[E], T[F], T[G], T[H])

}
