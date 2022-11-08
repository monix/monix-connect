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

package monix.connect.mongodb.internal

import com.mongodb.client.model._
import com.mongodb.reactivestreams.client.MongoCollection
import monix.connect.mongodb.domain._
import monix.eval.Task
import monix.execution.internal.InternalApi
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters._

/**
  * Internal implementation of the different Mongo Single Operations.
  */
@InternalApi
private[mongodb] class MongoSingleImpl {

  protected[this] def deleteOne[Doc](collection: MongoCollection[Doc], filter: Bson): Task[DeleteResult] =
    Task
      .fromReactivePublisher(collection.deleteOne(filter))
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultDeleteResult))

  protected[this] def deleteOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    deleteOptions: DeleteOptions,
    retryStrategy: RetryStrategy): Task[DeleteResult] =
    retryOnFailure(collection.deleteOne(filter, deleteOptions), retryStrategy)
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultDeleteResult))

  protected[this] def deleteMany[Doc](collection: MongoCollection[Doc], filter: Bson): Task[DeleteResult] =
    Task
      .fromReactivePublisher(collection.deleteMany(filter))
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultDeleteResult))

  protected[this] def deleteMany[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    deleteOptions: DeleteOptions,
    retryStrategy: RetryStrategy): Task[DeleteResult] =
    retryOnFailure(collection.deleteMany(filter, deleteOptions), retryStrategy)
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultDeleteResult))

  protected[this] def insertOne[Doc](collection: MongoCollection[Doc], document: Doc): Task[InsertOneResult] =
    Task
      .fromReactivePublisher(collection.insertOne(document))
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultInsertOneResult))

  protected[this] def insertOne[Doc](
    collection: MongoCollection[Doc],
    document: Doc,
    insertOneOptions: InsertOneOptions,
    retryStrategy: RetryStrategy): Task[InsertOneResult] =
    retryOnFailure(collection.insertOne(document, insertOneOptions), retryStrategy)
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultInsertOneResult))

  protected[this] def insertMany[Doc](collection: MongoCollection[Doc], docs: Seq[Doc]): Task[InsertManyResult] =
    Task
      .fromReactivePublisher(collection.insertMany(docs.asJava))
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultInsertManyResult))

  protected[this] def insertMany[Doc](
    collection: MongoCollection[Doc],
    docs: Seq[Doc],
    insertManyOptions: InsertManyOptions,
    retryStrategy: RetryStrategy): Task[InsertManyResult] =
    retryOnFailure(collection.insertMany(docs.asJava, insertManyOptions), retryStrategy)
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultInsertManyResult))

  protected[this] def replaceOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc): Task[UpdateResult] =
    Task
      .fromReactivePublisher(collection.replaceOne(filter, replacement))
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultUpdateResult))

  protected[this] def replaceOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc,
    replaceOptions: ReplaceOptions,
    retryStrategy: RetryStrategy): Task[UpdateResult] =
    retryOnFailure(collection.replaceOne(filter, replacement, replaceOptions), retryStrategy)
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultUpdateResult))

  protected[this] def updateOne[Doc](collection: MongoCollection[Doc], filter: Bson, update: Bson): Task[UpdateResult] =
    Task
      .fromReactivePublisher(collection.updateOne(filter, update))
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultUpdateResult))

  protected[this] def updateOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    updateOptions: UpdateOptions,
    retryStrategy: RetryStrategy): Task[UpdateResult] =
    retryOnFailure(collection.updateOne(filter, update, updateOptions), retryStrategy)
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultUpdateResult))

  protected[this] def updateMany[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson): Task[UpdateResult] =
    Task
      .fromReactivePublisher(collection.updateMany(filter, update))
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultUpdateResult))

  protected[this] def updateMany[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    updateOptions: UpdateOptions,
    retryStrategy: RetryStrategy): Task[UpdateResult] =
    retryOnFailure(collection.updateMany(filter, update, updateOptions), retryStrategy)
      .map(_.map(ResultConverter.fromJava).getOrElse(DefaultUpdateResult))

  protected[this] def createIndex[Doc](
    collection: MongoCollection[Doc],
    key: Bson,
    indexOptions: IndexOptions): Task[Unit] =
    Task
      .fromReactivePublisher(collection.createIndex(key, indexOptions))
      .flatMap(_ => Task.unit)

  protected[this] def createIndexes[Doc](
    collection: MongoCollection[Doc],
    indexes: List[IndexModel],
    createIndexOptions: CreateIndexOptions): Task[Unit] =
    Task
      .fromReactivePublisher(collection.createIndexes(indexes.asJava, createIndexOptions))
      .flatMap(_ => Task.unit)
}
