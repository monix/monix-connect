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

import com.mongodb.client.model.{DeleteOptions, InsertManyOptions, InsertOneOptions, ReplaceOptions, UpdateOptions}
import monix.execution.{Ack, Callback, Scheduler}
import monix.execution.cancelables.AssignableCancelable
import monix.reactive.Consumer
import monix.reactive.observers.Subscriber
import com.mongodb.reactivestreams.client.MongoCollection
import monix.eval.Coeval
import monix.execution.internal.InternalApi
import monix.connect.mongodb.domain.{DefaultDeleteOptions, DefaultInsertManyOptions, DefaultInsertOneOptions, DefaultReplaceOptions, DefaultRetryStrategy, DefaultUpdateOptions, RetryStrategy}
import org.bson.conversions.Bson
import org.reactivestreams.Publisher

import scala.jdk.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * A pre-built Monix [[Consumer]] implementation representing a Sink that expects events
  * of type [[A]] and executes the mongodb operation [[op]] passed to the class constructor.
  *
  * @param op                the mongodb operation defined as that expects an event of type [[A]] and
  *                          returns a reactivestreams [[Publisher]] of [[Any]].
  * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
  * @tparam A the type that the [[Consumer]] expects to receive
  */
@InternalApi private[mongodb] class MongoSink[A](
  op: A => Publisher[_],
  retryStrategy: RetryStrategy)
  extends Consumer[A, Unit] {

  override def createSubscriber(cb: Callback[Throwable, Unit], s: Scheduler): (Subscriber[A], AssignableCancelable) = {
    val sub = new Subscriber[A] {

      implicit val scheduler = s

      def onNext(request: A): Future[Ack] = {
        retryOnFailure(op(request), retryStrategy)
          .redeem(ex => {
            onError(ex)
            Ack.Stop
          }, _ => {
            Ack.Continue
          })
          .runToFuture
      }

      def onComplete(): Unit = {
        cb.onSuccess()
      }

      def onError(ex: Throwable): Unit = {
        cb.onError(ex)
      }
    }
    (sub, AssignableCancelable.single())
  }

}

/**
  * Companion object and factory for building a predefined [[MongoSink]].
  *
  * The current sinks available are (delete, insert, replace and update),
  * all of them available for `one` and `many` elements at a time.
  *
  */
object MongoSink {

  /**
    * Provides Sink for [[MongoSingle.deleteOne]] that for each incoming element will remove
    * at most one document from the collection that matches the given filter.
    *
    * @param collection        the abstraction to work with a determined MongoDB Collection
    * @param deleteOptions     the options to apply to all the delete operations, it will use default ones in case
    *                          it is not passed by the user
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that expects query filters to apply the the delete operations.
    * @see [[com.mongodb.client.model.Filters]] and [[com.mongodb.client.model.Updates]]
    */
  def deleteOne[Doc](
    collection: MongoCollection[Doc],
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Bson, Unit] = {
    val deleteOneOp = (filter: Bson) => collection.deleteOne(filter, deleteOptions)
    new MongoSink[Bson](deleteOneOp, retryStrategy)
  }

  /**
    * Provides Sink for [[MongoSingle.deleteMany]] that per each element removes all documents
    * from the collection that matched the given query filter.
    *
    * @param collection        the abstraction to work with a determined MongoDB Collection
    * @param deleteOptions     the options to apply to the delete operation, it will use default ones in case
    *                          it is not passed by the user.
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that expects query filters to apply the the delete many operations.
    * @see [[com.mongodb.client.model.Filters]]
    */
  def deleteMany[Doc](
    collection: MongoCollection[Doc],
    deleteOptions: DeleteOptions = DefaultDeleteOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Bson, Unit] = {
    val deleteManyOnNext = (filter: Bson) => collection.deleteMany(filter, deleteOptions)
    new MongoSink[Bson](deleteManyOnNext, retryStrategy)
  }

  /**
    * Provides a Sink for [[MongoSingle.insertOne]] that expects documents to be passed
    * and inserts them one by one.
    * If the document is missing an identifier, the driver should generate one.
    *
    * @param collection        the abstraction to work with the determined mongodb collection
    * @param insertOneOptions  the options to apply all the insert operations
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection and the incoming documents
    * @return a [[Consumer]] that expects single documents of type [[Doc]] to be inserted.
    */
  def insertOne[Doc](
    collection: MongoCollection[Doc],
    insertOneOptions: InsertOneOptions = DefaultInsertOneOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Doc, Unit] = {
    val insertOneOp = (document: Doc) => collection.insertOne(document, insertOneOptions)
    new MongoSink[Doc](insertOneOp, retryStrategy)
  }

  /**
    * Provides a Sink for [[MongoSingle.insertMany]] that expects batches of documents to be inserted at once.
    * If the documents is missing an identifier, the driver should generate one.
    *
    * @param collection        the abstraction to work with the determined mongodb collection
    * @param insertManyOptions the options to apply to the insert operation
    * @param retryStrategy defines the amount of retries and backoff delays for failed requests.
    * @tparam Doc the type of the collection
    * @return a [[Consumer]] that expects documents in batches of type [[Doc]] to be inserted.
    */
  def insertMany[Doc](
    collection: MongoCollection[Doc],
    insertManyOptions: InsertManyOptions = DefaultInsertManyOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[Seq[Doc], Unit] = {
    val insertOneOp = (documents: Seq[Doc]) => collection.insertMany(documents.asJava, insertManyOptions)
    new MongoSink[Seq[Doc]](insertOneOp, retryStrategy)
  }

  /**
    * Provides a Sink for [[MongoSingle.replaceOne]] that expects [[Tuple2]] of a filter and
    * the document replacement that for each element will execute the replace operation
    * to a single filtered element.
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
  def replaceOne[Doc](
    collection: MongoCollection[Doc],
    replaceOptions: ReplaceOptions = DefaultReplaceOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Doc), Unit] = {
    val replaceOp = (t: Tuple2[Bson, Doc]) => collection.replaceOne(t._1, t._2, replaceOptions)
    new MongoSink[(Bson, Doc)](replaceOp, retryStrategy)
  }

  /**
    * Provides a Sink for [[MongoSingle.updateOne]] that expects [[Tuple2]] of a filter and update
    * and that for each element will execute the update to a single filtered element.
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
  def updateOne[Doc](
    collection: MongoCollection[Doc],
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Bson), Unit] = {
    val updateOp = (t: Tuple2[Bson, Bson]) => collection.updateOne(t._1, t._2, updateOptions)
    new MongoSink[(Bson, Bson)](updateOp, retryStrategy)
  }

  /**
    * Provides a Sink for [[MongoSingle.updateOne]] that expects [[Tuple2]] of a filter and update
    * and that for each element will execute the update to all the filtered element.
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
  def updateMany[Doc](
    collection: MongoCollection[Doc],
    updateOptions: UpdateOptions = DefaultUpdateOptions,
    retryStrategy: RetryStrategy = DefaultRetryStrategy): Consumer[(Bson, Bson), Unit] = {
    val updateOp = (t: Tuple2[Bson, Bson]) => collection.updateMany(t._1, t._2, updateOptions)
    new MongoSink[(Bson, Bson)](updateOp, retryStrategy)
  }

}
