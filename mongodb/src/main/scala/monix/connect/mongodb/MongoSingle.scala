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

import com.mongodb.client.model._
import com.mongodb.reactivestreams.client.MongoCollection
import monix.connect.mongodb.domain.{
  DefaultCreateIndexesOptions,
  DefaultDeleteOptions,
  DefaultIndexOptions,
  DefaultInsertManyOptions,
  DefaultInsertOneOptions,
  DefaultReplaceOptions,
  DefaultRetryStrategy,
  DefaultUpdateOptions,
  DeleteResult,
  InsertManyResult,
  InsertOneResult,
  RetryStrategy,
  UpdateResult
}
import monix.connect.mongodb.internal.MongoSingleImpl
import monix.eval.Task
import org.bson.conversions.Bson

/**
  * Provides an idiomatic api for performing single operations against MongoDb.
  * It only exposes methods for appending and modifying (delete, insert, replace and update).
  */
object MongoSingle extends MongoSingleImpl {

  def apply[Doc](collection: MongoCollection[Doc]): MongoSingle[Doc] = new MongoSingle(collection)

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
  override def deleteOne[Doc](collection: MongoCollection[Doc], filter: Bson): Task[DeleteResult] =
    super.deleteOne(collection, filter)

  /**
    * Removes at most one document from the collection that matches the given filter with some delete options.
    * If no documents match, the collection is not modified.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection
    * @param filter the query filter to apply the the delete operation
    *               @see [[com.mongodb.client.model.Filters]]
    * @param deleteOptions the options to apply to the delete operation, it will use default ones in case
    *                      it is not passed by the user.
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  override def deleteOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[DeleteResult] =
    super.deleteOne(collection, filter, deleteOptions, retryStrategy)

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
  override def deleteMany[Doc](collection: MongoCollection[Doc], filter: Bson): Task[DeleteResult] =
    super.deleteMany(collection, filter)

  /**
    * Removes all documents from the collection that match the given query filter.
    * If no documents match, the collection is not modified.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection
    * @param filter the query filter to apply the the delete operation
    *               @see [[com.mongodb.client.model.Filters]]
    * @param deleteOptions the options to apply to the delete operation, it will use default ones in case
    *                      it is not passed by the user.
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional of [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  override def deleteMany[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[DeleteResult] =
    super.deleteMany(collection, filter, deleteOptions, retryStrategy)

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
  override def insertOne[Doc](collection: MongoCollection[Doc], document: Doc): Task[InsertOneResult] =
    super.insertOne(collection, document)

  /**
    * Inserts the provided document.
    * If the document is missing an identifier, the driver should generate one.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param document the document to be inserted
    * @param insertOneOptions the options to apply to the insert operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with the [[InsertOneResult]] that will contain the inserted id in case
    *         the operation finished successfully, being by default [[DefaultInsertOneResult]],
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  override def insertOne[Doc](
    collection: MongoCollection[Doc],
    document: Doc,
    insertOneOptions: InsertOneOptions = DefaultInsertOneOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[InsertOneResult] =
    super.insertOne(collection, document, insertOneOptions, retryStrategy)

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
  override def insertMany[Doc](collection: MongoCollection[Doc], docs: Seq[Doc]): Task[InsertManyResult] =
    super.insertMany(collection, docs)

  /**
    * Inserts a batch of documents.
    * If the documents is missing an identifier, the driver should generate one.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param docs the documents to insert
    * @param insertManyOptions the options to apply to the insert operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with the [[InsertManyResult]] that will contain the successful inserted ids,
    *         being by default [[DefaultInsertManyResult]],
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  override def insertMany[Doc](
    collection: MongoCollection[Doc],
    docs: Seq[Doc],
    insertManyOptions: InsertManyOptions = DefaultInsertManyOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[InsertManyResult] =
    super.insertMany(collection, docs, insertManyOptions, retryStrategy)

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
  override def replaceOne[Doc](collection: MongoCollection[Doc], filter: Bson, replacement: Doc): Task[UpdateResult] =
    super.replaceOne(collection, filter, replacement)

  /**
    * Replace a document in the collection according to the specified arguments.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to apply the the replace operation
    *               @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @param replaceOptions the options to apply to the replace operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with a single [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  override def replaceOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc,
    replaceOptions: ReplaceOptions = DefaultReplaceOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateResult] =
    super.replaceOne(collection, filter, replacement, replaceOptions, retryStrategy)

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
  override def updateOne[Doc](collection: MongoCollection[Doc], filter: Bson, update: Bson): Task[UpdateResult] =
    super.updateOne(collection, filter, update)

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
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  override def updateOne[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateResult] =
    super.updateOne(collection, filter, update, updateOptions, retryStrategy)

  /**
    * Update all documents in the collection according to the specified arguments.
    *
    * @param collection the abstraction to work with the determined mongodb Collection.
    * @param filter a document describing the query filter, which may not be null.
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null,
    *               the update to apply must include only update operators
    *               @see [[com.mongodb.client.model.Updates]]
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  override def updateMany[Doc](collection: MongoCollection[Doc], filter: Bson, update: Bson): Task[UpdateResult] =
    super.updateMany(collection, filter, update)

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
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional [[UpdateResult]] being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  override def updateMany[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateResult] =
    super.updateMany(collection, filter, update, updateOptions, retryStrategy)

  /**
    * Creates an index.
    *
    * @param collection   the abstraction to work with the determined mongodb Collection.
    * @param key          an object describing the index key(s), which may not be null.
    * @param indexOptions the options for the index
    * @tparam Doc the type of the collection
    * @return an empty [[Task]] or a failed one
    */
  override def createIndex[Doc](
    collection: MongoCollection[Doc],
    key: Bson,
    indexOptions: IndexOptions = DefaultIndexOptions): Task[Unit] =
    super.createIndex(collection, key, indexOptions)

  /**
    * Create multiple indexes.
    *
    * @param collection   the abstraction to work with the determined mongodb Collection.
    * @param indexes            the list of indexes
    * @see [[com.mongodb.client.model.IndexModel]]
    * @param createIndexOptions the options to use when creating indexes
    * @tparam Doc the type of the collection
    * @return an empty [[Task]] or a failed one
    */
  override def createIndexes[Doc](
    collection: MongoCollection[Doc],
    indexes: List[IndexModel],
    createIndexOptions: CreateIndexOptions = DefaultCreateIndexesOptions): Task[Unit] =
    super.createIndexes(collection, indexes, createIndexOptions)
}

class MongoSingle[Doc](private[mongodb] val collection: MongoCollection[Doc]) extends MongoSingleImpl {

  /**
    * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
    * modified.
    *
    * @param filter     the query filter to apply the delete operation.
    * @see [[com.mongodb.client.model.Filters]]
    * @return a [[Task]] with a [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  def deleteOne(filter: Bson): Task[DeleteResult] =
    super.deleteOne[Doc](collection, filter)

  /**
    * Removes at most one document from the collection that matches the given filter with some delete options.
    * If no documents match, the collection is not modified.
    *
    * @param filter the query filter to apply the the delete operation
    * @see [[com.mongodb.client.model.Filters]]
    * @param deleteOptions     the options to apply to the delete operation, it will use default ones in case
    *                          it is not passed by the user.
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    *                          by default no delay is applied.
    * @return a [[Task]] with an optional [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  def deleteOne(
    filter: Bson,
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[DeleteResult] =
    super.deleteOne(collection, filter, deleteOptions, retryStrategy)

  /**
    * Removes all documents from the collection that match the given query filter.
    * If no documents match, the collection is not modified.
    *
    * @param filter the query filter to apply the the delete operation
    * @return a [[Task]] with an optional [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  def deleteMany(filter: Bson): Task[DeleteResult] =
    super.deleteMany(collection, filter)

  /**
    * Removes all documents from the collection that match the given query filter.
    * If no documents match, the collection is not modified.
    *
    * @param filter the query filter to apply the the delete operation
    * @see [[com.mongodb.client.model.Filters]]
    * @param deleteOptions     the options to apply to the delete operation, it will use default ones in case
    *                          it is not passed by the user.
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with an optional of [[DeleteResult]], being by default [[DefaultDeleteResult]],
    *         or a failed one with [[com.mongodb.MongoException]].
    */
  def deleteMany(
    filter: Bson,
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[DeleteResult] =
    super.deleteMany(collection, filter, deleteOptions, retryStrategy)

  /**
    * Inserts the provided document.
    * If the document is missing an identifier, the driver should generate one.
    *
    * @param document the document to be inserted
    * @return a [[Task]] with the [[InsertOneResult]] that will contain the inserted id in case
    *         the operation finished successfully, being by default [[DefaultInsertOneResult]],
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  def insertOne(document: Doc): Task[InsertOneResult] =
    super.insertOne(collection, document)

  /**
    * Inserts the provided document.
    * If the document is missing an identifier, the driver should generate one.
    *
    * @param document          the document to be inserted
    * @param insertOneOptions  the options to apply to the insert operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with the [[InsertOneResult]] that will contain the inserted id in case
    *         the operation finished successfully, being by default [[DefaultInsertOneResult]],
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  def insertOne(
    document: Doc,
    insertOneOptions: InsertOneOptions = DefaultInsertOneOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[InsertOneResult] =
    super.insertOne(collection, document, insertOneOptions, retryStrategy)

  /**
    * Inserts a batch of documents.
    * If the documents is missing an identifier, the driver should generate one.
    *
    * @return a [[Task]] with the [[InsertManyResult]] that will contain the successful inserted ids,
    *         being by default [[DefaultInsertManyResult]].
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  def insertMany(docs: Seq[Doc]): Task[InsertManyResult] =
    super.insertMany(collection, docs)

  /**
    * Inserts a batch of documents.
    * If the documents is missing an identifier, the driver should generate one.
    *
    * @param docs              the documents to insert
    * @param insertManyOptions the options to apply to the insert operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with the [[InsertManyResult]] that will contain the successful inserted ids,
    *         being by default [[DefaultInsertManyResult]],
    *         or a failed one with [[com.mongodb.DuplicateKeyException]] or [[com.mongodb.MongoException]].
    */
  def insertMany(
    docs: Seq[Doc],
    insertManyOptions: InsertManyOptions = DefaultInsertManyOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[InsertManyResult] =
    super.insertMany(collection, docs, insertManyOptions, retryStrategy)

  /**
    * Replace a document in the collection according to the specified arguments.
    *
    * @param filter the query filter to apply the the replace operation
    * @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @return a [[Task]] with an [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def replaceOne(filter: Bson, replacement: Doc): Task[UpdateResult] =
    super.replaceOne(collection, filter, replacement)

  /**
    * Replace a document in the collection according to the specified arguments.
    *
    * @param filter the query filter to apply the the replace operation
    * @see [[com.mongodb.client.model.Filters]]
    * @param replacement       the replacement document
    * @param replaceOptions    the options to apply to the replace operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with a single [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def replaceOne(
    filter: Bson,
    replacement: Doc,
    replaceOptions: ReplaceOptions = DefaultReplaceOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateResult] =
    super.replaceOne(collection, filter, replacement, replaceOptions, retryStrategy)

  /**
    * Update a single document in the collection according to the specified arguments.
    *
    * @param filter a document describing the query filter, which may not be null.
    * @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null,
    *               the update to apply must include only update operators
    * @see [[com.mongodb.client.model.Updates]]
    * @return a [[Task]] with a [[UpdateResult]], being by default [[DefaultUpdateResult]]
    *         or a failed one.
    */
  def updateOne(filter: Bson, update: Bson): Task[UpdateResult] =
    super.updateOne(collection, filter, update)

  /**
    * Update a single document in the collection according to the specified arguments.
    *
    * @param filter a document describing the query filter, which may not be null.
    * @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null,
    *               the update to apply must include only update operators
    * @see [[com.mongodb.client.model.Updates]]
    * @param updateOptions     the options to apply to the update operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with an optional [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def updateOne(
    filter: Bson,
    update: Bson,
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateResult] =
    super.updateOne(collection, filter, update, updateOptions, retryStrategy)

  /**
    * Update all documents in the collection according to the specified arguments.
    *
    * @param filter a document describing the query filter, which may not be null.
    * @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null,
    *               the update to apply must include only update operators
    * @see [[com.mongodb.client.model.Updates]]
    * @return a [[Task]] with an optional [[UpdateResult]], being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def updateMany(filter: Bson, update: Bson): Task[UpdateResult] =
    super.updateMany(collection, filter, update)

  /**
    * Update all documents in the collection according to the specified arguments.
    *
    * @param filter a document describing the query filter, which may not be null.
    * @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null,
    *               the update to apply must include only update operators
    * @see [[com.mongodb.client.model.Updates]]
    * @param updateOptions     the options to apply to the update operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with an optional [[UpdateResult]] being by default [[DefaultUpdateResult]],
    *         or a failed one.
    */
  def updateMany(
    filter: Bson,
    update: Bson,
    updateOptions: UpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[UpdateResult] = {
    super.updateMany(collection, filter, update, updateOptions, retryStrategy)
  }

  /**
    * Creates an index.
    *
    * @param key          an object describing the index key(s), which may not be null.
    * @param indexOptions the options for the index
    * @return an empty [[Task]] or a failed one
    */
  def createIndex(key: Bson, indexOptions: IndexOptions = DefaultIndexOptions): Task[Unit] =
    super.createIndex(collection, key, indexOptions)

  /**
    * Create multiple indexes.
    *
    * @param indexes            the list of indexes
    * @see [[com.mongodb.client.model.IndexModel]]
    * @param createIndexOptions the options to use when creating indexes
    * @return an empty [[Task]] or a failed one
    */
  def createIndexes(
    indexes: List[IndexModel],
    createIndexOptions: CreateIndexOptions = DefaultCreateIndexesOptions): Task[Unit] =
    super.createIndexes(collection, indexes, createIndexOptions)
}
