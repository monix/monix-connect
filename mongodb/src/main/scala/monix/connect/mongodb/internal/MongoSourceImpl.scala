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

import com.mongodb.client.model.{
  CountOptions,
  FindOneAndDeleteOptions,
  FindOneAndReplaceOptions,
  FindOneAndUpdateOptions
}
import com.mongodb.reactivestreams.client.MongoCollection
import monix.connect.mongodb.domain.RetryStrategy
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.Observable
import org.bson.Document
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters._

/**
  * Internal implementation of the different Mongo Single Operations.
  */
@InternalApi
private[mongodb] class MongoSourceImpl {

  protected[this] def aggregate[A, B](
    collection: MongoCollection[A],
    pipeline: Seq[Bson],
    clazz: Class[B]): Observable[B] =
    Observable.fromReactivePublisher(collection.aggregate(pipeline.asJava, clazz))

  protected[this] def aggregate[Doc](collection: MongoCollection[Doc], pipeline: Seq[Bson]): Observable[Document] =
    Observable.fromReactivePublisher(collection.aggregate(pipeline.asJava, classOf[Document]))

  /**
    * Gets the distinct values of the specified field name.
    */
  protected[this] def distinct[Doc, T](
    collection: MongoCollection[Doc],
    fieldName: String,
    clazz: Class[T]): Observable[T] =
    Observable.fromReactivePublisher(collection.distinct(fieldName, clazz))

  /**
    * Gets the distinct values of the specified field name.
    */
  protected[this] def distinct[Doc](collection: MongoCollection[Doc], fieldName: String, filter: Bson)(
    implicit
    m: Manifest[Doc]): Observable[Doc] =
    Observable.fromReactivePublisher(collection.distinct(fieldName, filter, m.runtimeClass.asInstanceOf[Class[Doc]]))

  /**
    * Counts all the documents in the collection.
    */
  protected[this] def countAll[Doc](collection: MongoCollection[Doc]): Task[Long] =
    Task.fromReactivePublisher(collection.countDocuments()).map(_.map(_.longValue).getOrElse(-1L))

  /**
    * Counts all the documents in the collection.
    */
  protected[this] def countAll[Doc](collection: MongoCollection[Doc], retryStrategy: RetryStrategy): Task[Long] =
    retryOnFailure(collection.countDocuments(), retryStrategy).map(_.map(_.longValue).getOrElse(-1L))

  /**
    * Counts the number of documents in the collection that matched the query filter.
    */
  protected[this] def count[Doc](collection: MongoCollection[Doc], filter: Bson): Task[Long] =
    Task.fromReactivePublisher(collection.countDocuments(filter)).map(_.map(_.longValue).getOrElse(-1L))

  /**
    * Counts the number of documents in the collection that matched the query filter.
    */
  protected[this] def count[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    countOptions: CountOptions,
    retryStrategy: RetryStrategy): Task[Long] =
    retryOnFailure(collection.countDocuments(filter, countOptions), retryStrategy)
      .map(_.map(_.longValue).getOrElse(-1L))

  /**
    * Finds all documents in the collection.
    */
  protected[this] def findAll[Doc](collection: MongoCollection[Doc]): Observable[Doc] =
    Observable.fromReactivePublisher(collection.find())

  /**
    * Finds the documents in the collection that matched the query filter.
    */
  protected[this] def find[Doc](collection: MongoCollection[Doc], filter: Bson): Observable[Doc] =
    Observable.fromReactivePublisher(collection.find(filter))

  /**
    * Atomically find a document and remove it.
    */
  protected[this] def findOneAndDelete[Doc](collection: MongoCollection[Doc], filter: Bson): Task[Option[Doc]] =
    Task.fromReactivePublisher(collection.findOneAndDelete(filter))

  /**
    * Atomically find a document and remove it.
    */
  protected[this] def findOneAndDelete[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    findOneAndDeleteOptions: FindOneAndDeleteOptions,
    retryStrategy: RetryStrategy): Task[Option[Doc]] =
    retryOnFailure(collection.findOneAndDelete(filter, findOneAndDeleteOptions), retryStrategy)

  /**
    * Atomically find a document and replace it.
    */
  protected[this] def findOneAndReplace[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc): Task[Option[Doc]] =
    Task.fromReactivePublisher(collection.findOneAndReplace(filter, replacement))

  /**
    * Atomically find a document and replace it.
    */
  protected[this] def findOneAndReplace[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    replacement: Doc,
    findOneAndReplaceOptions: FindOneAndReplaceOptions,
    retryStrategy: RetryStrategy): Task[Option[Doc]] =
    retryOnFailure(collection.findOneAndReplace(filter, replacement, findOneAndReplaceOptions), retryStrategy)

  /**
    * Atomically find a document and update it.
    */
  protected[this] def findOneAndUpdate[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson): Task[Option[Doc]] =
    Task.fromReactivePublisher(collection.findOneAndUpdate(filter, update))

  /**
    * Atomically find a document and update it.
    */
  protected[this] def findOneAndUpdate[Doc](
    collection: MongoCollection[Doc],
    filter: Bson,
    update: Bson,
    findOneAndUpdateOptions: FindOneAndUpdateOptions,
    retryStrategy: RetryStrategy): Task[Option[Doc]] =
    retryOnFailure(collection.findOneAndUpdate(filter, update, findOneAndUpdateOptions), retryStrategy)

}
