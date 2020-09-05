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
  DefaultFindOneAndUpdateOptions
}
import com.mongodb.reactivestreams.client.MongoCollection
import monix.eval.{Coeval, Task}
import monix.reactive.Observable
import org.bson.Document
import org.bson.conversions.Bson

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
  * An object exposing those MongoDb signatures that basically aims to fetch data
  * from the collections by performing different type of read queries available
  * such like find, count, distinct or any type of aggregation.
  * There are three exceptions in which the method alters the data apart of fetching it,
  * which are the findOne and delete, replace or update.
  */
object MongoSource {

  /**
    * Aggregates documents according to the specified aggregation pipeline.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection
    * @param pipeline the aggregate pipeline
    * @param clazz the class to decode each document into
    * @tparam A the type that this collection will decode documents from
    * @tparam B the returned type result of the aggregation
    * @return an [[Observable]] of type [[B]], containing the result of the aggregation operation
    */
  def aggregate[A, B](collection: MongoCollection[A], pipeline: Seq[Bson], clazz: Class[B]): Observable[B] =
    Observable.fromReactivePublisher(collection.aggregate(pipeline.asJava, clazz))

  /**
    * Aggregates documents according to the specified aggregation pipeline.
    *
    * @param collection the abstraction to work with a determined MongoDB Collection.
    * @param pipeline the aggregate pipeline.
    * @tparam Doc the type that this collection will decode documents from.
    * @return
    */
  def aggregate[Doc](collection: MongoCollection[Doc], pipeline: Seq[Bson]): Observable[Document] =
    Observable.fromReactivePublisher(collection.aggregate(pipeline.asJava, classOf[Document]))

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
  def distinct[Doc, T](collection: MongoCollection[Doc], fieldName: String, clazz: Class[T]): Observable[T] =
    Observable.fromReactivePublisher(collection.distinct(fieldName, clazz))

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
  def distinct[Doc](collection: MongoCollection[Doc], fieldName: String, filter: Bson)(
    implicit
    m: Manifest[Doc]): Observable[Doc] =
    Observable.fromReactivePublisher(collection.distinct(fieldName, filter, m.runtimeClass.asInstanceOf[Class[Doc]]))

  /**
    * Counts all the documents in the collection.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @tparam Doc the type of the collection
    * @return a [[Task]] with a long indicating the number of documents, the result can be -1 if the underlying publisher
    *         did not emitted any documents, or a failed one when emitted an error.
    */
  def countAll[Doc](collection: MongoCollection[Doc]): Task[Long] =
    Task.fromReactivePublisher(collection.countDocuments()).map(_.map(_.longValue).getOrElse(-1L))

  /**
    * Counts the number of documents in the collection that matched the query filter.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter a document describing the query filter
    *               @see [[com.mongodb.client.model.Filters]]
    * @tparam Doc the type of the collection
    * @return a [[Task]] with a long indicating the number of documents, the result can be -1 if the underlying publisher
    *         did not emitted any documents, or a failed one when emitted an error.
    */
  def count[Doc](collection: MongoCollection[Doc], filter: Bson): Task[Long] =
    Task.fromReactivePublisher(collection.countDocuments(filter)).map(_.map(_.longValue).getOrElse(-1L))

  /**
    * Counts the number of documents in the collection that matched the query filter.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter a document describing the query filter
    *               @see [[com.mongodb.client.model.Filters]]
    * @param countOptions the options to apply to the count operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure
    * @tparam Doc the type of the collection
    * @param delayAfterFailure the delay set after the execution of a single operation failed.
    * @return a [[Task]] with a long indicating the number of documents, the result can be -1 if the underlying publisher
    *         did not emitted any documents, or a failed one when emitted an error.
    */
  def count[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    countOptions: CountOptions = DefaultCountOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[Long] =
    retryOnFailure(Coeval(collection.countDocuments(filter, countOptions)), retries, timeout, delayAfterFailure)
      .map(_.map(_.longValue).getOrElse(-1L))

  /**
    * Finds all documents in the collection.
    *
    * @param collection the abstraction to work with a determined mongodb collection
    * @tparam Doc the type of the collection
    * @return
    */
  def findAll[Doc](collection: MongoCollection[Doc]): Observable[Doc] =
    Observable.fromReactivePublisher(collection.find())

  /**
    * Finds the documents in the collection that matched the query filter.
    *
    * @param collection the abstraction to work with a determined mongodb collection
    * @param filter a document describing the query filter.
    *               @see [[com.mongodb.client.model.Filters]]
    * @tparam Doc the type of the collection
    * @return
    */
  def find[Doc](collection: MongoCollection[Doc], filter: Bson): Observable[Doc] =
    Observable.fromReactivePublisher(collection.find(filter))

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
  def findOneAndDelete[Doc](collection: MongoCollection[Doc], filter: Bson): Task[Option[Doc]] =
    Task.fromReactivePublisher(collection.findOneAndDelete(filter))

  /**
    * Atomically find a document and remove it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param findOneAndDeleteOptions the options to apply to the operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure
    * @param delayAfterFailure the delay set after the execution of a single operation failed.
    * @tparam Doc the type of the collection
    * @return a [[Task]] containing an optional of the document type that was removed
    *         if no documents matched the query filter it returns an empty option.
    */
  def findOneAndDelete[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    findOneAndDeleteOptions: FindOneAndDeleteOptions = DefaultFindOneAndDeleteOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[Option[Doc]] =
    retryOnFailure(
      Coeval(collection.findOneAndDelete(filter, findOneAndDeleteOptions)),
      retries,
      timeout,
      delayAfterFailure)

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
  def findOneAndReplace[Doc](collection: MongoCollection[Doc], filter: Bson, replacement: Doc): Task[Option[Doc]] =
    Task.fromReactivePublisher(collection.findOneAndReplace(filter, replacement))

  /**
    * Atomically find a document and replace it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param replacement the replacement document
    * @param findOneAndReplaceOptions the options to apply to the operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure
    * @param delayAfterFailure the delay set after the execution of a single operation failed.
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional of the document document that was replaced.
    *         If no documents matched the query filter, then an empty option will be returned.
    */
  def findOneAndReplace[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc,
    findOneAndReplaceOptions: FindOneAndReplaceOptions = DefaultFindOneAndReplaceOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[Option[Doc]] =
    retryOnFailure(
      Coeval(collection.findOneAndReplace(filter, replacement, findOneAndReplaceOptions)),
      retries,
      timeout,
      delayAfterFailure)

  /**
    * Atomically find a document and update it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null.
    *               The update to apply must include only update operators
    * @tparam Doc the type of the collection
    * @return a [[Task]] with an optional of the document that was updated before the update was applied.
    *         If no documents matched the query filter, then an empty option will be returned.
    */
  def findOneAndUpdate[Doc](collection: MongoCollection[Doc], filter: Bson, update: Bson): Task[Option[Doc]] =
    Task.fromReactivePublisher(collection.findOneAndUpdate(filter, update))

  /**
    * Atomically find a document and update it.
    *
    * @param collection the abstraction to work with the determined mongodb collection
    * @param filter the query filter to find the document with
    *               @see [[com.mongodb.client.model.Filters]]
    * @param update a document describing the update, which may not be null.
    *               The update to apply must include only update operators
    * @param findOneAndUpdateOptions the options to apply to the operation
    * @param retries the number of times the operation will be retried in case of unexpected failure,
    *                being zero retries by default
    * @param timeout expected timeout that the operation is expected to be executed or else return a failure
    * @param delayAfterFailure the delay set after the execution of a single operation failed.
    * @tparam Doc the type of the collection
    * @return
    */
  def findOneAndUpdate[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    findOneAndUpdateOptions: FindOneAndUpdateOptions = DefaultFindOneAndUpdateOptions,
    retries: Int = 0,
    timeout: Option[FiniteDuration] = Option.empty,
    delayAfterFailure: Option[FiniteDuration] = Option.empty): Task[Option[Doc]] =
    retryOnFailure(
      Coeval(collection.findOneAndUpdate(filter, update, findOneAndUpdateOptions)),
      retries,
      timeout,
      delayAfterFailure)

}
