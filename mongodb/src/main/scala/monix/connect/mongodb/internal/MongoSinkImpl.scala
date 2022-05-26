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

import com.mongodb.client.model.{DeleteOptions, InsertManyOptions, InsertOneOptions, ReplaceOptions, UpdateOptions}
import com.mongodb.reactivestreams.client.MongoCollection
import monix.connect.mongodb.domain.RetryStrategy
import monix.reactive.Consumer
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters._

/**
  * Internal implementation of the different available Mongo Sinks.
  */
private[mongodb] class MongoSinkImpl {

  protected[this] def deleteOne[Doc](
    collection: MongoCollection[Doc],
    deleteOptions: DeleteOptions,
    retryStrategy: RetryStrategy): Consumer[Bson, Unit] = {
    val deleteOneOp = (filter: Bson) => collection.deleteOne(filter, deleteOptions)
    new MongoSinkSubscriber(deleteOneOp, retryStrategy)
  }

  protected[this] def deleteOnePar[Doc](
    collection: MongoCollection[Doc],
    deleteOptions: DeleteOptions,
    retryStrategy: RetryStrategy): Consumer[List[Bson], Unit] = {
    val deleteOneOp = (filter: Bson) => collection.deleteOne(filter, deleteOptions)
    new MongoSinkParSubscriber(deleteOneOp, retryStrategy)
  }

  protected[this] def deleteMany[Doc](
    collection: MongoCollection[Doc],
    deleteOptions: DeleteOptions,
    retryStrategy: RetryStrategy): Consumer[Bson, Unit] = {
    val deleteManyOnNext = (filter: Bson) => collection.deleteMany(filter, deleteOptions)
    new MongoSinkSubscriber(deleteManyOnNext, retryStrategy)
  }

  protected[this] def insertOne[Doc](
    collection: MongoCollection[Doc],
    insertOneOptions: InsertOneOptions,
    retryStrategy: RetryStrategy): Consumer[Doc, Unit] = {
    val insertOneOp = (document: Doc) => collection.insertOne(document, insertOneOptions)
    new MongoSinkSubscriber(insertOneOp, retryStrategy)
  }

  protected[this] def insertOnePar[Doc](
                                      collection: MongoCollection[Doc],
                                      insertOneOptions: InsertOneOptions,
                                      retryStrategy: RetryStrategy): Consumer[List[Doc], Unit] = {
    val insertOneOp = (document: Doc) => collection.insertOne(document, insertOneOptions)
    new MongoSinkParSubscriber(insertOneOp, retryStrategy)
  }

  protected[this] def insertMany[Doc](
    collection: MongoCollection[Doc],
    insertManyOptions: InsertManyOptions,
    retryStrategy: RetryStrategy): Consumer[Seq[Doc], Unit] = {
    val insertOneOp = (documents: Seq[Doc]) => collection.insertMany(documents.asJava, insertManyOptions)
    new MongoSinkSubscriber(insertOneOp, retryStrategy)
  }

  protected[this] def replaceOne[Doc](
    collection: MongoCollection[Doc],
    replaceOptions: ReplaceOptions,
    retryStrategy: RetryStrategy): Consumer[(Bson, Doc), Unit] = {
    val replaceOp = (t: (Bson, Doc)) => collection.replaceOne(t._1, t._2, replaceOptions)
    new MongoSinkSubscriber(replaceOp, retryStrategy)
  }

  protected[this] def updateOne[Doc](
    collection: MongoCollection[Doc],
    updateOptions: UpdateOptions,
    retryStrategy: RetryStrategy): Consumer[(Bson, Bson), Unit] = {
    val updateOp = (t: (Bson, Bson)) => collection.updateOne(t._1, t._2, updateOptions)
    new MongoSinkSubscriber(updateOp, retryStrategy)
  }

  protected[this] def updateMany[Doc](
    collection: MongoCollection[Doc],
    updateOptions: UpdateOptions,
    retryStrategy: RetryStrategy): Consumer[(Bson, Bson), Unit] = {
    val updateOp = (t: (Bson, Bson)) => collection.updateMany(t._1, t._2, updateOptions)
    new MongoSinkSubscriber(updateOp, retryStrategy)
  }

}
