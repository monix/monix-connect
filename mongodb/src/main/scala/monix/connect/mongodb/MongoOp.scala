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

import com.mongodb.client.model.{DeleteOptions, InsertManyOptions, InsertOneOptions, ReplaceOptions, UpdateOptions}
import monix.connect.mongodb.domain.{
  DefaultDeleteOptions,
  DefaultDeleteResult,
  DefaultInsertManyOptions,
  DefaultInsertManyResult,
  DefaultInsertOneOptions,
  DefaultInsertOneResult,
  DefaultReplaceOptions,
  DefaultUpdateOptions,
  DefaultUpdateResult,
  DeleteResult,
  InsertManyResult,
  InsertOneResult,
  UpdateResult
}
import com.mongodb.reactivestreams.client.MongoCollection
import monix.eval.{Coeval, Task}
import org.bson.conversions.Bson

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * Provides an idiomatic api for performing single operations against MongoDb.
  * It only exposes methods for appending and modifying (delete, insert, replace and update).
  */
@deprecated("moved to `MongoSingle`", "0.5.3")
object MongoOp {

  /**
    * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
    * modified.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection.
    * @param filter the query filter to apply the delete operation.
    *               @see [[com.mongodb.client.model.Filters]]
    * @tparam Doc the type of the collection.
    * @return a [[Task]] with a [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  def deleteOne[Doc](collection: MongoCollection[Doc], filter: Bson): Task[DeleteResult] =
    Task
      .fromReactivePublisher(collection.deleteOne(filter))
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultDeleteResult))

  /**
    * Removes at most one document from the collection that matches the given filter with some delete options.
    * If no documents match, the collection is not modified.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection
    * @param filter the query filter to apply the the delete operation
    *               @see [[com.mongodb.client.model.Filters]]
    * @param deleteOptions the options to apply to the delete operation, it will use default ones in case
    *                      it is not passed by the user.
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default.
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure.
    * @param delayAfterFailure the delay set after the execution of a single operation failed,
    *                          by default no delay is applied.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  def deleteOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[DeleteResult] =
    retryOnFailure(Coeval(collection.deleteOne(filter, deleteOptions)), retries, timeout, delayAfterFailure)
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultDeleteResult))

  /**
    * Removes all documents from the collection that match the given query filter.
    * If no documents match, the collection is not modified.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection
    * @param filter the query filter to apply the the delete operation
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  def deleteMany[Doc](collection: MongoCollection[Doc], filter: Bson): Task[DeleteResult] =
    Task
      .fromReactivePublisher(collection.deleteMany(filter))
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultDeleteResult))

  /**
    * Removes all documents from the collection that match the given query filter.
    * If no documents match, the collection is not modified.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection
    * @param filter the query filter to apply the the delete operation
    *               @see [[com.mongodb.client.model.Filters]]
    * @param deleteOptions the options to apply to the delete operation, it will use default ones in case
    *                      it is not passed by the user.
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default.
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure.
    * @param delayAfterFailure the delay set after the execution of a single operation failed,
    *                          by default no delay is applied.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional of [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  def deleteMany[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[DeleteResult] =
    retryOnFailure(Coeval(collection.deleteMany(filter, deleteOptions)), retries, timeout, delayAfterFailure)
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultDeleteResult))

  /**
    * Inserts the provided document.
    * If the document is missing an identifier, the driver should generate one.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param document the document to be inserted
    * @tparam Doc the type of the collection
    * @return a [[Task]] with the [[InsertOneResult]] that will contain the inserted id in case
    *         the operation finished successfully, being by default [[DefaultInsertOneResult]],
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  def insertOne[Doc](collection: MongoCollection[Doc], document: Doc): Task[InsertOneResult] =
    Task
      .fromReactivePublisher(collection.insertOne(document))
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultInsertOneResult))

  /**
    * Inserts the provided document.
    * If the document is missing an identifier, the driver should generate one.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param document the document to be inserted
    * @param insertOneOptions the options to apply to the insert operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure
    * @param delayAfterFailure the delay set after the execution of a single operation failed,
    *                          by default no delay is applied.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with the [[InsertOneResult]] that will contain the inserted id in case
    *         the operation finished successfully, being by default [[DefaultInsertOneResult]],
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  def insertOne[Doc](
    collection: MongoCollection[Doc],
    document: Doc,
    insertOneOptions: InsertOneOptions = DefaultInsertOneOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[InsertOneResult] =
    retryOnFailure(Coeval(collection.insertOne(document, insertOneOptions)), retries, timeout, delayAfterFailure)
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultInsertOneResult))

  /**
    * Inserts a batch of documents.
    * If the documents is missing an identifier, the driver should generate one.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @tparam Doc the type of the collection
    * @return a [[Task]] with the [[InsertManyResult]] that will contain the successful inserted ids,
    *         being by default [[DefaultInsertManyResult]].
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  def insertMany[Doc](collection: MongoCollection[Doc], docs: Seq[Doc]): Task[InsertManyResult] =
    Task
      .fromReactivePublisher(collection.insertMany(docs.asJava))
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultInsertManyResult))

  /**
    * Inserts a batch of documents.
    * If the documents is missing an identifier, the driver should generate one.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param docs the documents to insert
    * @param insertManyOptions the options to apply to the insert operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure
    * @param delayAfterFailure the delay set after the execution of a single operation failed,
    *                          by default no delay is applied.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with the [[InsertManyResult]] that will contain the successful inserted ids,
    *         being by default [[DefaultInsertManyResult]],
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  def insertMany[Doc](
    collection: MongoCollection[Doc],
    docs: Seq[Doc],
    insertManyOptions: InsertManyOptions = DefaultInsertManyOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[InsertManyResult] =
    retryOnFailure(Coeval(collection.insertMany(docs.asJava, insertManyOptions)), retries, timeout, delayAfterFailure)
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultInsertManyResult))

  /**
    * Replace a document in the collection according to the specified arguments.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to apply the the replace operation
    *               @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def replaceOne[Doc](collection: MongoCollection[Doc], filter: Bson, replacement: Doc): Task[UpdateResult] =
    Task
      .fromReactivePublisher(collection.replaceOne(filter, replacement))
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultUpdateResult))

  /**
    * Replace a document in the collection according to the specified arguments.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to apply the the replace operation
    *               @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @param replaceOptions the options to apply to the replace operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure
    * @param delayAfterFailure the delay set after the execution of a single operation failed,
    *                          by default no delay is applied.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with a single [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def replaceOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc,
    replaceOptions: ReplaceOptions = DefaultReplaceOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[UpdateResult] =
    retryOnFailure(
      Coeval(collection.replaceOne(filter, replacement, replaceOptions)),
      retries,
      timeout,
      delayAfterFailure)
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultUpdateResult))

  /**
    * Update a single document in the collection according to the specified arguments.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter a document describing the query filter, which may not be null.
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null,
    *               the update to apply must include only update operators
    *               @see [[com.mongodb.client.model.Updates]]
    * @tparam Doc the type of the collection
    * @return a [[Task]] with a [[UpdateResult]], being by default [[DefaultUpdateResult]]
    *         or a failed one.
    */
  def updateOne[Doc](collection: MongoCollection[Doc], filter: Bson, update: Bson): Task[UpdateResult] =
    Task
      .fromReactivePublisher(collection.updateOne(filter, update))
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultUpdateResult))

  /**
    * Update a single document in the collection according to the specified arguments.
    *
    * @param collection the abstraction to work with the determined mongodb Collection.
    * @param filter a document describing the query filter, which may not be null.
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null,
    *               the update to apply must include only update operators
    *               @see [[com.mongodb.client.model.Updates]]
    * @param updateOptions the options to apply to the update operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default.
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure.
    * @param delayAfterFailure the delay set after the execution of a single operation failed,
    *                          by default no delay is applied.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def updateOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[UpdateResult] =
    retryOnFailure(Coeval(collection.updateOne(filter, update, updateOptions)), retries, timeout, delayAfterFailure)
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultUpdateResult))

  /**
    * Update all documents in the collection according to the specified arguments.
    *
    * @param collection the abstraction to work with the determined mongodb Collection.
    * @param filter a document describing the query filter, which may not be null.
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def updateMany[Doc](collection: MongoCollection[Doc], filter: Bson, update: Bson): Task[UpdateResult] =
    Task
      .fromReactivePublisher(collection.updateMany(filter, update))
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultUpdateResult))

  /**
    * Update all documents in the collection according to the specified arguments.
    *
    * @param collection the abstraction to work with the determined mongodb Collection.
    * @param filter a document describing the query filter, which may not be null.
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null,
    *               the update to apply must include only update operators
    *               @see [[com.mongodb.client.model.Updates]]
    * @param updateOptions the options to apply to the update operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default.
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure.
    * @param delayAfterFailure the delay set after the execution of a single operation failed.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[UpdateResult]] being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def updateMany[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[UpdateResult] = {
    retryOnFailure(Coeval(collection.updateMany(filter, update, updateOptions)), retries, timeout, delayAfterFailure)
      .map(_.map(ResultConverter.fromJava(_)).getOrElse(DefaultUpdateResult))
  }

}
