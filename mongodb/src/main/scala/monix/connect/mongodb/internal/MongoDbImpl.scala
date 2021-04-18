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

import com.mongodb.MongoNamespace
import com.mongodb.reactivestreams.client.{MongoClient, MongoDatabase}
import monix.connect.mongodb.MongoDb
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.Observable

@InternalApi
private[mongodb] class MongoDbImpl {

  private[mongodb] def createIfNotExists(db: MongoDatabase, collectionName: String): Task[Unit] =
    for {
      exists <- this.existsCollection(db, collectionName)
      create <- if (!exists) this.createCollection(db, collectionName) else Task.unit
    } yield create

  protected[this] def createCollection(db: MongoDatabase, collectionName: String): Task[Unit] =
    Task.fromReactivePublisher(db.createCollection(collectionName)).map(_ => ())

  protected[this] def dropDatabase(db: MongoDatabase): Task[Unit] =
    Task.fromReactivePublisher(db.drop()).map(_ => ())

  protected[this] def dropCollection(db: MongoDatabase, collectionName: String): Task[Unit] =
    Task.fromReactivePublisher(db.getCollection(collectionName).drop()).map((_ => ()))

  protected[this] def existsCollection(db: MongoDatabase, collectionName: String): Task[Boolean] =
    this.listCollections(db).existsL(_ == collectionName)

  protected[this] def existsDatabase(client: MongoClient, dbName: String): Task[Boolean] =
    this.listDatabases(client).existsL(_ == dbName)

  protected[this] def renameCollection(
    db: MongoDatabase,
    oldCollectionName: String,
    newCollectionName: String): Task[Boolean] = {
    val namespace = new MongoNamespace(newCollectionName)
    Task.fromReactivePublisher(db.getCollection(oldCollectionName).renameCollection(namespace)).map(_.isDefined)
  }

  protected[this] def listCollections(db: MongoDatabase): Observable[String] =
    Observable.fromReactivePublisher(db.listCollectionNames())

  protected[this] def listDatabases(client: MongoClient): Observable[String] =
    Observable.fromReactivePublisher(client.listDatabaseNames())

}
