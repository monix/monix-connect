package monix.connect.mongodb

import com.mongodb.MongoNamespace
import com.mongodb.reactivestreams.client.{MongoClient, MongoDatabase}
import monix.eval.Task
import monix.execution.internal.InternalApi
import monix.reactive.Observable

/** Object for managing the mongo databases and collections */
object MongoDb {

  /**
    * Create a new collection with the given name.
    *
    * @param db the database
    * @param collectionName the name for the new collection to create
    * @return a unit that signals on completion.
    *
    */
  def createCollection(db: MongoDatabase, collectionName: String): Task[Unit] =
    Task.fromReactivePublisher(db.createCollection(collectionName)).map(_ => ())

  /**
    * Drops a database.
    *
    * @param db the database to be dropped
    * @return a unit that signals on completion.
    */
  def dropDatabase(db: MongoDatabase): Task[Unit] =
    Task.fromReactivePublisher(db.drop()).map(_ => ())

  /**
    * Drops a collection from the database.
    *
    * @param db the database
    * @param collectionName the name of the collection to drop
    * @return a unit that signals on completion
    */
  def dropCollection(db: MongoDatabase, collectionName: String): Task[Unit] =
    Task.fromReactivePublisher(db.getCollection(collectionName).drop()).map((_ => ()))

  /**
    * Check whether a collection exists or not.
    *
    * @param db the database
    * @param collectionName the name of the collection
    * @return a boolean [[Task]] indicating whether the collection exists or not.
    */
  def existsCollection(db: MongoDatabase, collectionName: String): Task[Boolean] =
    MongoDb.listCollections(db).filter(_ == collectionName).map(_ => true).headOrElseL(false)

  /**
    * Checks whether a database exists or not.
    *
    * @param client the client-side representation of a MongoDB cluster.
    * @param dbName the name of the database
    * @return a boolean [[Task]] indicating whether the database exists or not.
    */
  def existsDatabase(client: MongoClient, dbName: String): Task[Boolean] =
    MongoDb.listDatabases(client).filter(_ == dbName).map(_ => true).headOrElseL(false)

  /**
    * Rename the collection with oldCollectionName to the newCollectionName.
    *
    * @param db the database
    * @param oldCollectionName the current (old) name of the collection
    * @param newCollectionName the name (new) which the collection will be renamed to
    * @return a boolean [[Task]] indicating whether the collection was successfully renamed or not.
    */
  def renameCollection(db: MongoDatabase, oldCollectionName: String, newCollectionName: String): Task[Boolean] = {
    val namespace = new MongoNamespace(newCollectionName)
    Task.fromReactivePublisher(db.getCollection(oldCollectionName).renameCollection(namespace)).map(_.isDefined)
  }

  /**
    * Lists all the collections in the given database.
    *
    * @param db the database
    * @return an [[Observable]] that emits the names of all the existing collections.
    */
  def listCollections(db: MongoDatabase): Observable[String] =
    Observable.fromReactivePublisher(db.listCollectionNames())

  /**
    * Get a list of the database names
    *
    * @param client the client-side representation of a MongoDB cluster.
    * @return an [[Observable]] that emits the names of all the existing databases.
    */
  def listDatabases(client: MongoClient): Observable[String] =
    Observable.fromReactivePublisher(client.listDatabaseNames())

}