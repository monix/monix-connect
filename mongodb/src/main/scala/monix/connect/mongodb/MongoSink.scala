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
import monix.reactive.Consumer
import com.mongodb.reactivestreams.client.MongoCollection
import monix.connect.mongodb.domain.{
  DefaultDeleteOptions,
  DefaultInsertManyOptions,
  DefaultInsertOneOptions,
  DefaultReplaceOptions,
  DefaultRetryStrategy,
  DefaultUpdateOptions,
  RetryStrategy
}
import monix.connect.mongodb.internal.MongoSinkImpl
import org.bson.conversions.Bson

/**
  * Companion object and factory for building a predefined [[MongoSink]].
  *
  * The current sinks available are (delete, insert, replace and update),
  * all of them available for `one` and `many` elements at a time.
  *
  */
object MongoSink extends MongoSinkImpl {

  def apply[Doc](collection: MongoCollection[Doc]): MongoSink[Doc] = new MongoSink(collection)
  /**
    * Provides a sink implementation for [[MongoSingle.deleteOne]] that for each incoming element
    * will remove at most one document from the collection that matches the given filter.
    *
    * @param collection        the abstraction to work with a determined MongoDB Collection
    * @param deleteOptions     the options to apply to all the delete operations, it will use default ones in case
    *                          it is not passed by the user
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that expects query filters to apply the the delete operations.
    * @see [[com.mongodb.client.model.Filters]] and [[com.mongodb.client.model.Updates]]
    */
  override def deleteOne[Doc](
    collection: MongoCollection[Doc],
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Bson, Unit] = {
    super.deleteOne(collection, deleteOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.deleteMany]] that per each element
    * removes all documents from the collection that matched the given query filter.
    *
    * @param collection        the abstraction to work with a determined MongoDB Collection
    * @param deleteOptions     the options to apply to the delete operation, it will use default ones in case
    *                          it is not passed by the user.
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that expects query filters to apply the the delete many operations.
    * @see [[com.mongodb.client.model.Filters]]
    */
  override def deleteMany[Doc](
    collection: MongoCollection[Doc],
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Bson, Unit] =
    super.deleteMany(collection, deleteOptions, retryStrategy)

  /**
    * Provides a sink implementation for [[MongoSingle.insertOne]] that
    * expects documents to be passed and inserts them one by one.
    * If the document is missing an identifier, the driver should generate one.
    *
    * @param collection        the abstraction to work with the determined mongodb collection
    * @param insertOneOptions  the options to apply all the insert operations
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection and the incoming documents
    * @return a [[Consumer]] that expects single documents of type [[Doc]] to be inserted.
    */
  override def insertOne[Doc](
    collection: MongoCollection[Doc],
    insertOneOptions: InsertOneOptions = DefaultInsertOneOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Doc, Unit] = {
    super.insertOne(collection, insertOneOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.insertMany]] that expects
    * batches of documents to be inserted at once.
    * If the documents is missing an identifier, the driver should generate one.
    *
    * @param collection        the abstraction to work with the determined mongodb collection
    * @param insertManyOptions the options to apply to the insert operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that expects documents in batches of type [[Doc]] to be inserted.
    */
  override def insertMany[Doc](
    collection: MongoCollection[Doc],
    insertManyOptions: InsertManyOptions = DefaultInsertManyOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Seq[Doc], Unit] = {
    super.insertMany(collection, insertManyOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.replaceOne]] that expects
    * [[Tuple2]] of a filter and the document replacement that for each element
    * will execute the replace operation to a single filtered element.
    *
    * @see [[com.mongodb.client.model.Filters]]
    *
    *      If the documents is missing an identifier, the driver should generate one.
    * @param collection        the abstraction to work with the determined mongodb collection
    * @param replaceOptions    the options to apply to the replace operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that expects a [[Tuple2]] of a filter and a document of type [[Doc]] to be replaced.
    */
  override def replaceOne[Doc](
    collection: MongoCollection[Doc],
    replaceOptions: ReplaceOptions = DefaultReplaceOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Doc), Unit] = {
    super.replaceOne(collection, replaceOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.updateOne]] that expects [[Tuple2]]
    * of a filter and a update that will be executed against the single filtered element.
    *
    * @see [[com.mongodb.client.model.Filters]] and [[com.mongodb.client.model.Updates]]
    *
    *      If the documents is missing an identifier, the driver should generate one.
    * @param collection        the abstraction to work with the determined mongodb collection
    * @param updateOptions     the options to apply to the update operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that per each element expects a [[Tuple2]] of a filter and the update in form of [[Bson]].
    */
  override def updateOne[Doc](
    collection: MongoCollection[Doc],
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Bson), Unit] = {
    super.updateOne(collection, updateOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.updateOne]] that expects [[Tuple2]]
    * of a filter and update that will be executed against all the filtered elements.
    *
    * @see [[com.mongodb.client.model.Filters]] and [[com.mongodb.client.model.Updates]]
    *
    *      If the documents is missing an identifier, the driver should generate one.
    * @param collection        the abstraction to work with the determined mongodb collection
    * @param updateOptions     the options to apply to the update operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that per each element expects [[Tuple2]] of a filter and the update in form of [[Bson]].
    */
  override def updateMany[Doc](
    collection: MongoCollection[Doc],
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Bson), Unit] = {
    super.updateMany(collection, updateOptions, retryStrategy)
  }

}

class MongoSink[Doc](private[mongodb] val collection: MongoCollection[Doc]) extends MongoSinkImpl {

  /**
    * Provides a sink implementation for [[MongoSingle.deleteOne]] that for each incoming element
    * will remove at most one document from the collection that matches the given filter.
    *
    * @param deleteOptions     the options to apply to all the delete operations, it will use default ones in case
    *                          it is not passed by the user
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Consumer]] that expects query filters to apply the the delete operations.
    * @see [[com.mongodb.client.model.Filters]] and [[com.mongodb.client.model.Updates]]
    */
  def deleteOne(
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Bson, Unit] = {
    super.deleteOne(collection, deleteOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.deleteMany]] that per each element
    * removes all documents from the collection that matched the given query filter.
    *
    * @param deleteOptions     the options to apply to the delete operation, it will use default ones in case
    *                          it is not passed by the user.
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Consumer]] that expects query filters to apply the the delete many operations.
    * @see [[com.mongodb.client.model.Filters]]
    */
  def deleteMany(
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Bson, Unit] =
    super.deleteMany(collection, deleteOptions, retryStrategy)

  /**
    * Provides a sink implementation for [[MongoSingle.insertOne]] that
    * expects documents to be passed and inserts them one by one.
    * If the document is missing an identifier, the driver should generate one.
    *
    * @param insertOneOptions  the options to apply all the insert operations
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Consumer]] that expects single documents of type [[Doc]] to be inserted.
    */
  def insertOne(
    insertOneOptions: InsertOneOptions = DefaultInsertOneOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Doc, Unit] = {
    super.insertOne(collection, insertOneOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.insertMany]]
    * that expects batches of documents to be inserted at once.
    * If the documents is missing an identifier, the driver should generate one.
    *
    * @param insertManyOptions the options to apply to the insert operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Consumer]] that expects documents in batches of type [[Doc]] to be inserted.
    */
  def insertMany(
    insertManyOptions: InsertManyOptions = DefaultInsertManyOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Seq[Doc], Unit] = {
    super.insertMany(collection, insertManyOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.replaceOne]] that expects
    * [[Tuple2]] of a filter and the document replacement that for each element
    * will execute the replace operation to a single filtered element.
    *
    * @see [[com.mongodb.client.model.Filters]]
    *
    *      If the documents is missing an identifier, the driver should generate one.
    * @param replaceOptions    the options to apply to the replace operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Consumer]] that expects a [[Tuple2]] of a filter and a document of type [[Doc]] to be replaced.
    */
  def replaceOne(
    replaceOptions: ReplaceOptions = DefaultReplaceOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Doc), Unit] = {
    super.replaceOne(collection, replaceOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.updateOne]] that expects [[Tuple2]]
    * of a filter and a update that will be executed against the single filtered element.
    *
    * @see [[com.mongodb.client.model.Filters]] and [[com.mongodb.client.model.Updates]]
    *
    *      If the documents is missing an identifier, the driver should generate one.
    * @param updateOptions     the options to apply to the update operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Consumer]] that per each element expects a [[Tuple2]] of a filter and the update in form of [[Bson]].
    */
  def updateOne(
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Bson), Unit] = {
    super.updateOne(collection, updateOptions, retryStrategy)
  }

  /**
    * Provides a sink implementation for [[MongoSingle.updateOne]] that expects [[Tuple2]]
    * of a filter and update that will be executed against all the filtered elements.
    *
    * @see [[com.mongodb.client.model.Filters]] and [[com.mongodb.client.model.Updates]]
    *
    *      If the documents is missing an identifier, the driver should generate one.
    * @param updateOptions     the options to apply to the update operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @return a [[Consumer]] that per each element expects [[Tuple2]] of a filter and the update in form of [[Bson]].
    */
  def updateMany(
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Bson), Unit] = {
    super.updateMany(collection, updateOptions, retryStrategy)
  }

}
