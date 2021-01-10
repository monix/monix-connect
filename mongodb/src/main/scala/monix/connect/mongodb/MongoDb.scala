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

import com.mongodb.reactivestreams.client.{MongoClient, MongoDatabase}
import monix.connect.mongodb.internal.MongoDbImpl
import monix.eval.Task
import monix.reactive.Observable

/** Object for managing the mongo databases and collections */
object MongoDb extends MongoDbImpl {

  def apply(client: MongoClient, db: MongoDatabase): MongoDb = new MongoDb(client, db)

  /**
    * Create a new collection with the given name.
    *
    * @param db the database
    * @param collectionName the name for the new collection to create
    * @return a unit that signals on completion.
    *
    */
  override def createCollection(db: MongoDatabase, collectionName: String): Task[Unit] =
    super.createCollection(db, collectionName)

  /**
    * Drops a database.
    *
    * @param db the database to be dropped
    * @return a unit that signals on completion.
    */
  override def dropDatabase(db: MongoDatabase): Task[Unit] =
    super.dropDatabase(db)

  /**
    * Drops a collection from the database.
    *
    * @param db the database
    * @param collectionName the name of the collection to drop
    * @return a unit that signals on completion
    */
  override def dropCollection(db: MongoDatabase, collectionName: String): Task[Unit] =
    super.dropCollection(db, collectionName)

  /**
    * Check whether a collection exists or not.
    *
    * @param db the database
    * @param collectionName the name of the collection
    * @return a boolean [[Task]] indicating whether the collection exists or not.
    */
  override def existsCollection(db: MongoDatabase, collectionName: String): Task[Boolean] =
    super.existsCollection(db, collectionName)

  /**
    * Checks whether a database exists or not.
    *
    * @param client the client-side representation of a MongoDB cluster.
    * @param dbName the name of the database
    * @return a boolean [[Task]] indicating whether the database exists or not.
    */
  override def existsDatabase(client: MongoClient, dbName: String): Task[Boolean] =
    super.existsDatabase(client, dbName)

  /**
    * Rename the collection with oldCollectionName to the newCollectionName.
    *
    * @param db the database
    * @param oldCollectionName the current (old) name of the collection
    * @param newCollectionName the name (new) which the collection will be renamed to
    * @return a boolean [[Task]] indicating whether the collection was successfully renamed or not.
    */
  override def renameCollection(
    db: MongoDatabase,
    oldCollectionName: String,
    newCollectionName: String): Task[Boolean] =
    super.renameCollection(db, oldCollectionName, newCollectionName)

  /**
    * Lists all the collections in the given database.
    *
    * @param db the database
    * @return an [[Observable]] that emits the names of all the existing collections.
    */
  override def listCollections(db: MongoDatabase): Observable[String] =
    super.listCollections(db)

  /**
    * Get a list of the database names
    *
    * @param client the client-side representation of a MongoDB cluster.
    * @return an [[Observable]] that emits the names of all the existing databases.
    */
  override def listDatabases(client: MongoClient): Observable[String] =
    super.listDatabases(client)

}

class MongoDb(private[mongodb] val client: MongoClient, private[mongodb] val db: MongoDatabase) extends MongoDbImpl {

  /**
    * Create a new collection with the given name.
    *
    * @param collectionName the name for the new collection to create
    * @return a unit that signals on completion.
    *
    */
  def createCollection(collectionName: String): Task[Unit] =
    super.createCollection(db, collectionName)

  /**
    * Drops a database.
    *
    * @return a unit that signals on completion.
    */
  def dropDatabase(): Task[Unit] =
    super.dropDatabase(db)

  /**
    * Drops a collection from the database.
    *
    * @param collectionName the name of the collection to drop
    * @return a unit that signals on completion
    */
  def dropCollection(collectionName: String): Task[Unit] =
    super.dropCollection(db, collectionName)

  /**
    * Check whether a collection exists or not.
    *
    * @param collectionName the name of the collection
    * @return a boolean [[Task]] indicating whether the collection exists or not.
    */
  def existsCollection(collectionName: String): Task[Boolean] =
    super.existsCollection(db, collectionName)

  /**
    * Checks whether a database exists or not.
    *
    * @param dbName the name of the database
    * @return a boolean [[Task]] indicating whether the database exists or not.
    */
  def existsDatabase(dbName: String): Task[Boolean] =
    super.existsDatabase(client, dbName)

  /**
    * Rename the collection with oldCollectionName to the newCollectionName.
    *
    * @param oldCollectionName the current (old) name of the collection
    * @param newCollectionName the name (new) which the collection will be renamed to
    * @return a boolean [[Task]] indicating whether the collection was successfully renamed or not.
    */
  def renameCollection(oldCollectionName: String, newCollectionName: String): Task[Boolean] =
    super.renameCollection(db, oldCollectionName, newCollectionName)

  /**
    * Lists all the collections in the given database.
    *
    * @return an [[Observable]] that emits the names of all the existing collections.
    */
  def listCollections(): Observable[String] =
    super.listCollections(db)

  /**
    * Get the list of the all database names.
    *
    * @return an [[Observable]] that emits the names of all the existing databases.
    */
  def listAllDatabases(): Observable[String] =
    super.listDatabases(client)
}
