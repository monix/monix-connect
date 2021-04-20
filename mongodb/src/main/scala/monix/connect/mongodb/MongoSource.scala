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
  FindOneAndDeleteOptions,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions
}
import monix.connect.mongodb.domain.{
  DefaultCountOptions,
  DefaultFindOneAndDeleteOptions,
  DefaultFindOneAndReplaceOptions,
  DefaultFindOneAndUpdateOptions,
  DefaultRetryStrategy,
  RetryStrategy
}
import com.mongodb.reactivestreams.client.MongoCollection
import monix.connect.mongodb.internal.MongoSourceImpl
import monix.eval.Task
import monix.reactive.Observable
import org.bson.Document
import org.bson.conversions.Bson

/**
  * An object that exposes those MongoDb definitions for fetching data
  * from collections by performing different type of read queries available
  * such like find, count, distinct or aggregation.
  * There are three exceptions in which the method also alters the data apart
  * of reading it, which are the findOne and delete, replace or update.
  */
object MongoSource extends MongoSourceImpl {

  def apply[Doc](collection: MongoCollection[Doc]): MongoSource[Doc] = new MongoSource(collection)

  /**
    * Aggregates documents according to the specified aggregation pipeline.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection
    * @param pipeline the aggregate pipeline
    * @param clazz the class to decode each document into
    * @tparam A the type that this collection will decode documents from
    * @tparam B the returned type result of the aggregation
    * @return an [[Observable]] of type [[B]], containing the result of the aggregation pipeline
    */
  override def aggregate[A, B](collection: MongoCollection[A], pipeline: Seq[Bson], clazz: Class[B]): Observable[B] =
    super.aggregate(collection, pipeline, clazz)

  /**
    * Aggregates documents according to the specified aggregation pipeline.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection.
    * @param pipeline the aggregate pipeline.
    * @tparam Doc the type that this collection will decode documents from.
    * @return an [[Observable]] of type [[Document]], containing the result of the aggregation pipeline
    */
  override def aggregate[Doc](collection: MongoCollection[Doc], pipeline: Seq[Bson]): Observable[Document] =
    super.aggregate(collection, pipeline)

  /**
    * Gets the distinct values of the specified field name.
    *
    * @param collection the abstraction to work with a determined mongodb collection
    * @param fieldName the document's field name
    * @param clazz the class to decode each document into
    * @tparam Doc the type of the collection
    * @tparam T the type of the field which the distinct operation is pointing to
    * @return an [[Observable]] that emits the distinct the distinct values of type [[Doc]]
    */
  override def distinct[Doc, T](collection: MongoCollection[Doc], fieldName: String, clazz: Class[T]): Observable[T] =
    super.distinct(collection, fieldName, clazz)

  /**
    * Gets the distinct values of the specified field name.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param fieldName the document's field name
    * @param filter a document describing the query filter
    *               @see [[com.mongodb.client.model.Filters]]
    * @param m implicit manifest of type [[Doc]]
    * @tparam Doc the type of the collection
    * @return an [[Observable]] that emits the distinct the distinct values of type [[Doc]]
    */
  override def distinct[Doc](collection: MongoCollection[Doc], fieldName: String, filter: Bson)(
    implicit
    m: Manifest[Doc]): Observable[Doc] =
    super.distinct(collection, fieldName, filter)

  /**
    * Counts all the documents in the collection.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @tparam Doc the type of the collection
    * @return a [[Task]] with a long indicating the number of documents
    *         the result will be -1 if the underlying publisher did not emitted any documents,
    *         or a failed one when emitted an error.
    */
  override def countAll[Doc](collection: MongoCollection[Doc]): Task[Long] =
    super.countAll(collection)

  /**
    * Counts all the documents in the collection.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @tparam Doc the type of the collection
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with a long indicating the number of documents
    *         the result will be -1 if the underlying publisher did not emitted any documents,
    *         or a failed one when emitted an error.
    */
  override def countAll[Doc](
    collection: MongoCollection[Doc],
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Long] =
    super.countAll(collection, retryStrategy)

  /**
    * Counts the number of documents in the collection that matched the query filter.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter a document describing the query filter
    *               @see [[com.mongodb.client.model.Filters]]
    * @tparam Doc the type of the collection
    * @return a [[Task]] with a long indicating the number of documents
    *         the result will be -1 if the underlying publisher did not emitted any documents,
    *         or a failed one when emitted an error.
    */
  override def count[Doc](collection: MongoCollection[Doc], filter: Bson): Task[Long] =
    super.count(collection, filter)

  /**
    * Counts the number of documents in the collection that matched the query filter.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter a document describing the query filter
    *               @see [[com.mongodb.client.model.Filters]]
    * @param countOptions the options to apply to the count operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with a long indicating the number of documents,
    *         the result can be -1 if the underlying publisher did not emitted any documents,
    *         or a failed one when emitted an error.
    */
  override def count[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    countOptions: CountOptions = DefaultCountOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Long] =
    super.count(collection, filter, countOptions, retryStrategy)

  /**
    * Finds all documents in the collection.
    *
    * @param collection the abstraction to work with a determined mongodb collection
    * @tparam Doc the type of the collection
    * @return all documents of type [[Doc]] within the collection
    */
  override def findAll[Doc](collection: MongoCollection[Doc]): Observable[Doc] =
    super.findAll(collection)

  /**
    * Finds the documents in the collection that matched the query filter.
    *
    * @param collection the abstraction to work with a determined mongodb collection
    * @param filter a document describing the query filter.
    *               @see [[com.mongodb.client.model.Filters]]
    * @tparam Doc the type of the collection
    * @return the documents that matched with the given filter
    */
  override def find[Doc](collection: MongoCollection[Doc], filter: Bson): Observable[Doc] =
    super.find(collection, filter)

  /**
    * Atomically find a document and remove it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @tparam Doc the type of the collection
    * @return a [[Task]] containing an optional of the document type that was removed
    *         if no documents matched the query filter it returns an empty option.
    */
  override def findOneAndDelete[Doc](collection: MongoCollection[Doc], filter: Bson): Task[Option[Doc]] =
    super.findOneAndDelete(collection, filter)

  /**
    * Atomically find a document and remove it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param findOneAndDeleteOptions the options to apply to the operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] containing an optional of the document type that was removed
    *         if no documents matched the query filter it returns an empty option.
    */
  override def findOneAndDelete[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    findOneAndDeleteOptions: FindOneAndDeleteOptions = DefaultFindOneAndDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Option[Doc]] =
    super.findOneAndDelete(collection, filter, findOneAndDeleteOptions, retryStrategy)

  /**
    * Atomically find a document and replace it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional of the document that was replaced.
    *         If no documents matched the query filter, then an empty option will be returned.
    */
  override def findOneAndReplace[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc): Task[Option[Doc]] =
    super.findOneAndReplace(collection, filter, replacement)

  /**
    * Atomically find a document and replace it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @param findOneAndReplaceOptions the options to apply to the operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional of the document document that was replaced.
    *         If no documents matched the query filter, then an empty option will be returned.
    */
  override def findOneAndReplace[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc,
    findOneAndReplaceOptions: FindOneAndReplaceOptions = DefaultFindOneAndReplaceOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Option[Doc]] =
    super.findOneAndReplace(collection, filter, replacement, findOneAndReplaceOptions, retryStrategy)

  /**
    * Atomically find a document and update it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null.
    *               The update to apply must include only update operators
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional of the document that was updated before the update was applied,
    *         if no documents matched the query filter, then an empty option will be returned.
    */
  override def findOneAndUpdate[Doc](collection: MongoCollection[Doc], filter: Bson, update: Bson): Task[Option[Doc]] =
    super.findOneAndUpdate(collection, filter, update)

  /**
    * Atomically find a document and update it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null.
    *               The update to apply must include only update operators
    * @param findOneAndUpdateOptions the options to apply to the operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional of the document that was updated before the update was applied,
    *         if no documents matched the query filter, then an empty option will be returned.
    */
  override def findOneAndUpdate[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    findOneAndUpdateOptions: FindOneAndUpdateOptions = DefaultFindOneAndUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Option[Doc]] =
    super.findOneAndUpdate(collection, filter, update, findOneAndUpdateOptions, retryStrategy)

}

class MongoSource[Doc](private[mongodb] val collection: MongoCollection[Doc]) extends MongoSourceImpl {

  /**
    * Aggregates documents according to the specified aggregation pipeline.
    *
    * @param pipeline the aggregate pipeline
    * @param clazz the class to decode each document into
    * @tparam T the returned type result of the aggregation
    * @return an [[Observable]] of type [[B]], containing the result of the aggregation pipeline
    */
  def aggregate[T](pipeline: Seq[Bson], clazz: Class[T]): Observable[T] =
    super.aggregate(collection, pipeline, clazz)

  /**
    * Aggregates documents according to the specified aggregation pipeline.
    *
    * @param pipeline the aggregate pipeline.
    * @return an [[Observable]] of type [[Document]], containing the result of the aggregation pipeline
    */
  def aggregate(pipeline: Seq[Bson]): Observable[Document] =
    super.aggregate(collection, pipeline)

  /**
    * Gets the distinct values of the specified field name.
    *
    * @param fieldName the document's field name
    * @param clazz the class to decode each document into
    * @tparam T the type of the field which the distinct operation is pointing to
    * @return an [[Observable]] that emits the distinct the distinct values of type [[Doc]]
    */
  def distinct[T](fieldName: String, clazz: Class[T]): Observable[T] =
    super.distinct(collection, fieldName, clazz)

  /**
    * Gets the distinct values of the specified field name.
    *
    * @param fieldName the document's field name
    * @param filter a document describing the query filter
    *               @see [[com.mongodb.client.model.Filters]]
    * @param m implicit manifest of type [[Doc]]
    * @return an [[Observable]] that emits the distinct the distinct values of type [[Doc]]
    */
  def distinct(fieldName: String, filter: Bson)(
    implicit
    m: Manifest[Doc]): Observable[Doc] =
    super.distinct(collection, fieldName, filter)

  /**
    * Counts all the documents in the collection.
    *
    * @return a [[Task]] with a long indicating the number of documents
    *         the result will be -1 if the underlying publisher did not emitted any documents,
    *         or a failed one when emitted an error.
    */
  def countAll: Task[Long] =
    super.countAll(collection)

  /**
    * Counts all the documents in the collection.
    *
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with a long indicating the number of documents
    *         the result will be -1 if the underlying publisher did not emitted any documents,
    *         or a failed one when emitted an error.
    */
  def countAll(retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Long] =
    super.countAll(collection, retryStrategy)

  /**
    * Counts the number of documents in the collection that matched the query filter.
    *
    * @param filter a document describing the query filter
    *               @see [[com.mongodb.client.model.Filters]]
    * @return a [[Task]] with a long indicating the number of documents
    *         the result will be -1 if the underlying publisher did not emitted any documents,
    *         or a failed one when emitted an error.
    */
  def count(filter: Bson): Task[Long] =
    super.count(collection, filter)

  /**
    * Counts the number of documents in the collection that matched the query filter.
    *
    * @param filter a document describing the query filter
    *               @see [[com.mongodb.client.model.Filters]]
    * @param countOptions the options to apply to the count operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with a long indicating the number of documents,
    *         the result can be -1 if the underlying publisher did not emitted any documents,
    *         or a failed one when emitted an error.
    */
  def count(
    filter: Bson,
    countOptions: CountOptions = DefaultCountOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Long] =
    super.count(collection, filter, countOptions, retryStrategy)

  /**
    * Finds all documents in the collection.
    *
    * @return all documents of type [[Doc]] within the collection
    */
  def findAll: Observable[Doc] =
    super.findAll(collection)

  /**
    * Finds the documents in the collection that matched the query filter.
    *
    * @param filter a document describing the query filter.
    *               @see [[com.mongodb.client.model.Filters]]
    * @return the documents that matched with the given filter
    */
  def find(filter: Bson): Observable[Doc] =
    super.find(collection, filter)

  /**
    * Atomically find a document and remove it.
    *
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @return a [[Task]] containing an optional of the document type that was removed
    *         if no documents matched the query filter it returns an empty option.
    */
  def findOneAndDelete(filter: Bson): Task[Option[Doc]] =
    super.findOneAndDelete(collection, filter)

  /**
    * Atomically find a document and remove it.
    *
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param findOneAndDeleteOptions the options to apply to the operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] containing an optional of the document type that was removed
    *         if no documents matched the query filter it returns an empty option.
    */
  def findOneAndDelete(
    filter: Bson,
    findOneAndDeleteOptions: FindOneAndDeleteOptions = DefaultFindOneAndDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Option[Doc]] =
    super.findOneAndDelete(collection, filter, findOneAndDeleteOptions, retryStrategy)

  /**
    * Atomically find a document and replace it.
    *
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @return a [[Task]] with an optional of the document that was replaced.
    *         If no documents matched the query filter, then an empty option will be returned.
    */
  def findOneAndReplace(filter: Bson, replacement: Doc): Task[Option[Doc]] =
    super.findOneAndReplace(collection, filter, replacement)

  /**
    * Atomically find a document and replace it.
    *
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @param findOneAndReplaceOptions the options to apply to the operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with an optional of the document document that was replaced.
    *         If no documents matched the query filter, then an empty option will be returned.
    */
  def findOneAndReplace(
    filter: Bson,
    replacement: Doc,
    findOneAndReplaceOptions: FindOneAndReplaceOptions = DefaultFindOneAndReplaceOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Option[Doc]] =
    super.findOneAndReplace(collection, filter, replacement, findOneAndReplaceOptions, retryStrategy)

  /**
    * Atomically find a document and update it.
    *
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null.
    *               The update to apply must include only update operators
    * @return a [[Task]] with an optional of the document that was updated before the update was applied,
    *         if no documents matched the query filter, then an empty option will be returned.
    */
  def findOneAndUpdate(filter: Bson, update: Bson): Task[Option[Doc]] =
    super.findOneAndUpdate(collection, filter, update)
  /**
    * Atomically find a document and update it.
    *
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null.
    *               The update to apply must include only update operators
    * @param findOneAndUpdateOptions the options to apply to the operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Task]] with an optional of the document that was updated before the update was applied,
    *         if no documents matched the query filter, then an empty option will be returned.
    */
  def findOneAndUpdate(
    filter: Bson,
    update: Bson,
    findOneAndUpdateOptions: FindOneAndUpdateOptions = DefaultFindOneAndUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Task[Option[Doc]] =
    super.findOneAndUpdate(collection, filter, update, findOneAndUpdateOptions, retryStrategy)

}
