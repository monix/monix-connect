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

import cats.effect.Resource
import com.mongodb.MongoClientSettings
import monix.connect.mongodb.domain.{MongoCollection, MongoConnector, Tuple2F, Tuple3F, Tuple4F, Tuple5F, Tuple6F, Tuple7F, Tuple8F}
import monix.eval.Task
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients, MongoDatabase}
import monix.execution.annotations.UnsafeBecauseImpure
import monix.reactive.Observable
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY

/**
  * Singleton object that exposes signatures to create a connection to the desired
  * specified mongo collections, the abstraction to operate with collections is
  * returned in form of [[MongoConnector]], which is based of three different
  * components, the db, source, single and sink.
  *
  * The aim is to provide a idiomatic interface for different operations that
  * can be run against a collection, for either read with [[MongoSource]] or to
  * write/delete one by one with [[MongoSingle]] or in streaming fashion
  * with the [[MongoSink]].
  *
  */
object MongoConnection {

  private[mongodb] def fromCodecProvider(codecRegistry: CodecProvider*): CodecRegistry =
    fromRegistries(fromProviders(codecRegistry: _*), DEFAULT_CODEC_REGISTRY)

  private[mongodb] def connection1[T1]: MongoConnection[MongoCollection[T1], MongoConnector[T1]] =
    new MongoConnection[MongoCollection[T1], MongoConnector[T1]] {

      override def createConnectors(client: MongoClient, collections: MongoCollection[T1]): Task[MongoConnector[T1]] = {
        val db: MongoDatabase = client.getDatabase(collections.databaseName)
        MongoDb.createIfNotExists(db, collections.collectionName).map { _ =>
          val col = db
            .getCollection(collections.collectionName, collections.clazz)
            .withCodecRegistry(fromCodecProvider(collections.codecProvider: _*))
          MongoConnector(MongoDb(client, db), MongoSource(col), MongoSingle(col), MongoSink(col))
        }
      }

      override def close(connectors: MongoConnector[T1]): Task[Unit] = connectors.close
    }

  private[mongodb] def connection2[T1, T2]
    : MongoConnection[Tuple2F[MongoCollection, T1, T2], Tuple2F[MongoConnector, T1, T2]] =
    (client: MongoClient, collections: Tuple2F[MongoCollection, T1, T2]) => {
      for {
        a <- connection1[T1](client, collections._1)
        b <- connection1[T2](client, collections._2)
      } yield (a, b)
    }

  private[mongodb] def connection3[T1, T2, T3]
    : MongoConnection[Tuple3F[MongoCollection, T1, T2, T3], Tuple3F[MongoConnector, T1, T2, T3]] =
    (client: MongoClient, collections: Tuple3F[MongoCollection, T1, T2, T3]) => {
      val (c1, c2, c3) = collections
      for {
        a <- connection1[T1](client, c1)
        b <- connection2[T2, T3].createConnectors(client, (c2, c3))
      } yield (a, b._1, b._2)
    }

  private[mongodb] def connection4[T1, T2, T3, T4]
    : MongoConnection[Tuple4F[MongoCollection, T1, T2, T3, T4], Tuple4F[MongoConnector, T1, T2, T3, T4]] =
    (client: MongoClient, collections: Tuple4F[MongoCollection, T1, T2, T3, T4]) => {
      val (c1, c2, c3, c4) = collections
      for {
        a <- connection1[T1](client, c1)
        b <- connection3[T2, T3, T4].createConnectors(client, (c2, c3, c4))
      } yield (a, b._1, b._2, b._3)
    }

  private[mongodb] def connection5[T1, T2, T3, T4, T5]
    : MongoConnection[Tuple5F[MongoCollection, T1, T2, T3, T4, T5], Tuple5F[MongoConnector, T1, T2, T3, T4, T5]] =
    (client: MongoClient, collections: Tuple5F[MongoCollection, T1, T2, T3, T4, T5]) => {
      val (c1, c2, c3, c4, c5) = collections

      for {
        a <- connection1[T1](client, c1)
        b <- connection4[T2, T3, T4, T5].createConnectors(client, (c2, c3, c4, c5))
      } yield (a, b._1, b._2, b._3, b._4)
    }

  private[mongodb] def connection6[T1, T2, T3, T4, T5, T6]: MongoConnection[
    Tuple6F[MongoCollection, T1, T2, T3, T4, T5, T6],
    Tuple6F[MongoConnector, T1, T2, T3, T4, T5, T6]] =
    (client: MongoClient, collections: Tuple6F[MongoCollection, T1, T2, T3, T4, T5, T6]) => {
      val (c1, c2, c3, c4, c5, c6) = collections
      for {
        a <- connection1[T1](client, c1)
        b <- connection5[T2, T3, T4, T5, T6].createConnectors(client, (c2, c3, c4, c5, c6))
      } yield (a, b._1, b._2, b._3, b._4, b._5)
    }

  private[mongodb] def connection7[T1, T2, T3, T4, T5, T6, T7]: MongoConnection[
    Tuple7F[MongoCollection, T1, T2, T3, T4, T5, T6, T7],
    Tuple7F[MongoConnector, T1, T2, T3, T4, T5, T6, T7]] =
    (client: MongoClient, collections: Tuple7F[MongoCollection, T1, T2, T3, T4, T5, T6, T7]) => {
      val (c1, c2, c3, c4, c5, c6, c7) = collections
      for {
        a <- connection1[T1](client, c1)
        b <- connection6[T2, T3, T4, T5, T6, T7].createConnectors(client, (c2, c3, c4, c5, c6, c7))
      } yield (a, b._1, b._2, b._3, b._4, b._5, b._6)
    }

  private[mongodb] def connection8[T1, T2, T3, T4, T5, T6, T7, T8]: MongoConnection[
    Tuple8F[MongoCollection, T1, T2, T3, T4, T5, T6, T7, T8],
    Tuple8F[MongoConnector, T1, T2, T3, T4, T5, T6, T7, T8]] =
    (client: MongoClient, collections: Tuple8F[MongoCollection, T1, T2, T3, T4, T5, T6, T7, T8]) => {
      val (c1, c2, c3, c4, c5, c6, c7, c8) = collections
      for {
        a <- connection1[T1](client, c1)
        b <- connection7[T2, T3, T4, T5, T6, T7, T8].createConnectors(client, (c2, c3, c4, c5, c6, c7, c8))
      } yield (a, b._1, b._2, b._3, b._4, b._5, b._6, b._7)
    }

  /**
    * Creates a single [[MongoConnector]] from the passed [[MongoCollection]].
    *
    * ==Example==
    * {{{
    *   import com.mongodb.client.model.Filters
    *   import monix.eval.Task
    *   import monix.connect.mongodb.domain.{MongoCollection, MongoConnector}
    *   import monix.connect.mongodb.MongoConnection
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *
    *   case class Employee(name: String, age: Int, companyName: String = "X")
    *
    *   val employee = Employee("Stephen", 32)
    *   val employeesCol = MongoCollection("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
    *   val connection = MongoConnection.create1("mongodb://localhost:27017", employeesCol)
    *
    *   val t: Task[Employee] =
    *   connection.use { case MongoConnector(db, source, single, sink) =>
    *     // business logic here
    *     single.insertOne(employee)
    *       .flatMap(_ => source.find(Filters.eq("name", employee.name)).headL)
    *   }
    * }}}
    *
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collection describes the collection that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create1[T1](connectionString: String, collection: MongoCollection[T1]): Resource[Task, MongoConnector[T1]] =
    connection1[T1].create(connectionString, collection)

  /**
    *
    * Creates a single [[MongoConnector]] from the passed [[MongoCollection]].
    *
    * ==Example==
    * {{{
    *   import com.mongodb.client.model.Filters
    *   import monix.eval.Task
    *   import com.mongodb.{MongoClientSettings, ServerAddress}
    *   import monix.connect.mongodb.MongoConnection
    *   import monix.connect.mongodb.domain.{MongoCollection, MongoConnector}
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *
    *   import scala.jdk.CollectionConverters._
    *
    *   case class Employee(name: String, age: Int, companyName: String = "X")
    *
    *   val employee = Employee("Stephen", 32)
    *   val employeesCol = MongoCollection("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
    *
    *   val mongoClientSettings = MongoClientSettings.builder
    *       .applyToClusterSettings(builder => builder.hosts(List(new ServerAddress("localhost", 27017)).asJava))
    *       .build
    *
    *   val connection = MongoConnection.create1(mongoClientSettings, employeesCol)
    *   val t: Task[Employee] =
    *   connection.use { case MongoConnector(db, source, single, sink) =>
    *     // business logic here
    *     single.insertOne(employee)
    *       .flatMap(_ => source.find(Filters.eq("name", employee.name)).headL)
    *   }
    * }}}
    *
    * @param clientSettings various settings to control the behavior the created [[MongoConnector]].
    * @param collection     describes the collection that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create1[T1](
    clientSettings: MongoClientSettings,
    collection: MongoCollection[T1]): Resource[Task, MongoConnector[T1]] =
    connection1[T1].create(clientSettings, collection)

  /**
    * Creates a single [[MongoConnector]] from the specified [[MongoCollection]].
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed, alternatively it will be released
    * and closed towards the usage of the resource task.
    * Always prefer to use [[create1]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collection describes the collection that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]
    */
  @UnsafeBecauseImpure
  def createUnsafe1[T1](client: MongoClient, collection: MongoCollection[T1]): Resource[Task, MongoConnector[T1]] =
    connection1[T1].createUnsafe(client, collection)

  /**
    * Creates a connection to mongodb and provides with a [[MongoConnector]]
    * for each of the *TWO* provided [[MongoCollection]]s.
    *
    * ==Example==
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.mongodb.domain.{MongoCollection, MongoConnector}
    *   import monix.connect.mongodb.MongoConnection
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *
    *   case class Employee(name: String, age: Int, companyName: String = "X")
    *   case class Company(name: String, employees: List[Employee], investment: Int = 0)
    *
    *   val employee1 = Employee("Gerard", 39)
    *   val employee2 = Employee("Laura", 41)
    *   val company = Company("Stephen", List(employee1, employee2))
    *
    *   val employeesCol = MongoCollection("business", "employees_collection", classOf[Employee], createCodecProvider[Employee]())
    *   val companiesCol = MongoCollection("business", "companies_collection", classOf[Company], createCodecProvider[Company](), createCodecProvider[Employee]())
    *
    *   val connection = MongoConnection.create2("mongodb://localhost:27017", (employeesCol, companiesCol))
    *
    *   val t: Task[Unit] =
    *   connection.use { case (MongoConnector(_, employeeSource, employeeSingle, employeeSink),
    *                          MongoConnector(_, companySource, companySingle, companySink)) =>
    *     // business logic here
    *     for {
    *       r1 <- employeeSingle.insertMany(List(employee1, employee2))
    *       r2 <- companySingle.insertOne(company)
    *     } yield ()
    *   }
    * }}}
    *
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]s.
    */
  def create2[T1, T2](
    connectionString: String,
    collections: Tuple2F[MongoCollection, T1, T2]): Resource[Task, Tuple2F[MongoConnector, T1, T2]] =
    connection2.create(connectionString, collections)

  /**
    * Creates a connection to mongodb and provides with a [[MongoConnector]]
    * for each of the *TWO* provided [[MongoCollection]]s.
    *
    * ==Example==
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.mongodb.domain.{MongoCollection, MongoConnector}
    *   import monix.connect.mongodb.MongoConnection
    *   import com.mongodb.{MongoClientSettings, ServerAddress}
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *   import scala.jdk.CollectionConverters._
    *
    *   case class Employee(name: String, age: Int, companyName: String = "X")
    *   case class Company(name: String, employees: List[Employee], investment: Int = 0)
    *
    *   val employee1 = Employee("Gerard", 39)
    *   val employee2 = Employee("Laura", 41)
    *   val company = Company("Stephen", List(employee1, employee2))
    *
    *   val employeesCol = MongoCollection("business", "employees_collection", classOf[Employee], createCodecProvider[Employee]())
    *   val companiesCol = MongoCollection("business", "companies_collection", classOf[Company], createCodecProvider[Company](), createCodecProvider[Employee]())
    *
    *   val mongoClientSettings = MongoClientSettings.builder
    *       .applyToClusterSettings(builder => builder.hosts(List(new ServerAddress("localhost", 27017)).asJava))
    *       .build
    *
    *   val connection = MongoConnection.create2(mongoClientSettings, (employeesCol, companiesCol))
    *
    *   val t: Task[Unit] =
    *   connection.use { case (MongoConnector(_, employeeSource, employeeSingle, employeeSink),
    *                          MongoConnector(_, companySource, companySingle, companySink)) =>
    *     // business logic here
    *     for {
    *       r1 <- employeeSingle.insertMany(List(employee1, employee2))
    *       r2 <- companySingle.insertOne(company)
    *     } yield ()
    *   }
    * }}}
    *
    * @param clientSettings various settings to control the behavior of the created [[MongoConnector]]s.
    * @param collections    describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create2[T1, T2](
    clientSettings: MongoClientSettings,
    collections: Tuple2F[MongoCollection, T1, T2]): Resource[Task, Tuple2F[MongoConnector, T1, T2]] =
    connection2.create(clientSettings, collections)

  /**
    * Unsafely creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *TWO* provided [[MongoCollection]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed, or alternatively it will be released
    * and closed towards the usage of the resource task.
    * Always prefer to use [[create2]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]
    */
  @UnsafeBecauseImpure
  def createUnsafe2[T1, T2](
    client: MongoClient,
    collections: Tuple2F[MongoCollection, T1, T2]): Resource[Task, Tuple2F[MongoConnector, T1, T2]] =
    connection2.createUnsafe(client, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * for each of the *THREE* provided [[MongoCollection]]s.
    *
    * ==Example==
    * {{{
    *   import com.mongodb.client.model.{Filters, Updates}
    *   import monix.eval.Task
    *   import monix.connect.mongodb.domain.{MongoCollection, UpdateResult}
    *   import monix.connect.mongodb.MongoConnection
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *
    *   import scala.concurrent.duration._
    *   import scala.jdk.CollectionConverters._
    *
    *   case class Employee(name: String, age: Int, companyName: String)
    *   case class Company(name: String, employees: List[Employee], investment: Int = 0)
    *   case class Investor(name: String, funds: Int, companies: List[Company])
    *
    *   val companiesCol = MongoCollection(
    *         "my_db",
    *         "companies_collection",
    *         classOf[Company],
    *         createCodecProvider[Company](),
    *         createCodecProvider[Employee]())
    *   val employeesCol =
    *     MongoCollection("my_db", "employees_collection", classOf[Employee], createCodecProvider[Employee]())
    *   val investorsCol = MongoCollection(
    *     "my_db",
    *     "investors_collection",
    *     classOf[Investor],
    *     createCodecProvider[Investor](),
    *     createCodecProvider[Company]())
    *
    *   val mongoEndpoint = "mongodb://localhost:27017"
    *   val connection = MongoConnection.create3(mongoEndpoint, (companiesCol, employeesCol, investorsCol))
    *
    *   //in this example we are trying to move the employees and investment
    *   //from an old company a the new one, presumably, there is already a `Company`
    *   //with name `OldCompany` which also have `Employee`s and `Investor`s.
    *
    *   val updateResult: Task[UpdateResult] = connection.use {
    *     case (
    *         companyConnector,
    *         employeeConnector,
    *         investorConnector) =>
    *       for {
    *         // creates the new company
    *         _ <- companyConnector.single.insertOne(Company("NewCompany", employees = List.empty, investment = 0)).delayResult(1.second)
    *         //read employees from old company and pushes them into the new one
    *         _ <- {
    *           employeeConnector
    *             .source
    *             .find(Filters.eq("companyName", "OldCompany"))
    *             .bufferTimedAndCounted(2.seconds, 15)
    *             .map { employees =>
    *               // pushes them into the new one
    *               (Filters.eq("name", "NewCompany"),
    *                 Updates.pushEach("employees", employees.asJava))
    *             }
    *             .consumeWith(companyConnector.sink.updateOne())
    *         }
    *         // sums all the investment funds of the old company and updates the total company's investment
    *         investment <- investorConnector.source.find(Filters.in("companies.name", "OldCompany")).map(_.funds).sumL
    *         updateResult <- companyConnector.single.updateMany(
    *           Filters.eq("name", "NewCompany"),
    *           Updates.set("investment", investment))
    *       } yield updateResult
    *   }
    * }}}
    *
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]s.
    */
  def create3[T1, T2, T3](
    connectionString: String,
    collections: Tuple3F[MongoCollection, T1, T2, T3]): Resource[Task, Tuple3F[MongoConnector, T1, T2, T3]] =
    connection3.create(connectionString, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * for each of the *THREE* provided [[MongoCollection]]s.
    *
    * @param clientSettings various settings to control the behavior of the created [[MongoConnector]]s.
    * @param collections    describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create3[T1, T2, T3](
    clientSettings: MongoClientSettings,
    collections: Tuple3F[MongoCollection, T1, T2, T3]): Resource[Task, Tuple3F[MongoConnector, T1, T2, T3]] =
    connection3.create(clientSettings, collections)

  /**
    * Unsafely creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *THREE* provided [[MongoCollection]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which will be released and closed towards the usage of the resource task.
    * Always prefer to use [[create3]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]
    */
  @UnsafeBecauseImpure
  def createUnsafe3[T1, T2, T3](
    client: MongoClient,
    collections: Tuple3F[MongoCollection, T1, T2, T3]): Resource[Task, Tuple3F[MongoConnector, T1, T2, T3]] =
    connection3.createUnsafe(client, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *FOUR* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information on how to configure it
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]s.
    */
  def create4[T1, T2, T3, T4](
    connectionString: String,
    collections: Tuple4F[MongoCollection, T1, T2, T3, T4]): Resource[Task, Tuple4F[MongoConnector, T1, T2, T3, T4]] =
    connection4.create(connectionString, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *FOUR* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[MongoConnector]]s.
    * @param collections    describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create4[T1, T2, T3, T4](
    clientSettings: MongoClientSettings,
    collections: Tuple4F[MongoCollection, T1, T2, T3, T4]): Resource[Task, Tuple4F[MongoConnector, T1, T2, T3, T4]] =
    connection4.create(clientSettings, collections)

  /**
    * Unsafely creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *FOUR* provided [[MongoCollection]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which will be released and closed towards the usage of the resource task.
    * Always prefer to use [[create4]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]
    */
  @UnsafeBecauseImpure
  def createUnsafe4[T1, T2, T3, T4](
    client: MongoClient,
    collections: Tuple4F[MongoCollection, T1, T2, T3, T4]): Resource[Task, Tuple4F[MongoConnector, T1, T2, T3, T4]] =
    connection4.createUnsafe(client, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *FIVE* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]s.
    */
  def create5[T1, T2, T3, T4, T5](connectionString: String, collections: Tuple5F[MongoCollection, T1, T2, T3, T4, T5])
    : Resource[Task, Tuple5F[MongoConnector, T1, T2, T3, T4, T5]] =
    connection5.create(connectionString, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *FIVE* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[MongoConnector]]s.
    * @param collections    describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create5[T1, T2, T3, T4, T5](
    clientSettings: MongoClientSettings,
    collections: Tuple5F[MongoCollection, T1, T2, T3, T4, T5])
    : Resource[Task, Tuple5F[MongoConnector, T1, T2, T3, T4, T5]] =
    connection5.create(clientSettings, collections)

  /**
    * Unsafely creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *FIVE* provided [[MongoCollection]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which will be released and closed towards the usage of the resource task.
    * Always prefer to use [[create5]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]
    */
  @UnsafeBecauseImpure
  def createUnsafe5[T1, T2, T3, T4, T5](client: MongoClient, collections: Tuple5F[MongoCollection, T1, T2, T3, T4, T5])
    : Resource[Task, Tuple5F[MongoConnector, T1, T2, T3, T4, T5]] =
    connection5.createUnsafe(client, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *SIX* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]s.
    */
  def create6[T1, T2, T3, T4, T5, T6](
    connectionString: String,
    collections: Tuple6F[MongoCollection, T1, T2, T3, T4, T5, T6])
    : Resource[Task, Tuple6F[MongoConnector, T1, T2, T3, T4, T5, T6]] =
    connection6.create(connectionString, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *SIX* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[MongoConnector]]s.
    * @param collections    describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create6[T1, T2, T3, T4, T5, T6](
    clientSettings: MongoClientSettings,
    collections: Tuple6F[MongoCollection, T1, T2, T3, T4, T5, T6])
    : Resource[Task, Tuple6F[MongoConnector, T1, T2, T3, T4, T5, T6]] =
    connection6.create(clientSettings, collections)

  /**
    * Unsafely creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *SIX* provided [[MongoCollection]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which will be released and closed towards the usage of the resource task.
    * Always prefer to use [[create6]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]
    */
  def createUnsafe6[T1, T2, T3, T4, T5, T6](
    client: MongoClient,
    collections: Tuple6F[MongoCollection, T1, T2, T3, T4, T5, T6])
    : Resource[Task, Tuple6F[MongoConnector, T1, T2, T3, T4, T5, T6]] =
    connection6.createUnsafe(client, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *SEVEN* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]s.
    */
  def create7[T1, T2, T3, T4, T5, T6, T7](
    connectionString: String,
    collections: Tuple7F[MongoCollection, T1, T2, T3, T4, T5, T6, T7])
    : Resource[Task, Tuple7F[MongoConnector, T1, T2, T3, T4, T5, T6, T7]] =
    connection7.create(connectionString, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *SEVEN* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[MongoConnector]]s.
    * @param collections    describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create7[T1, T2, T3, T4, T5, T6, T7](
    clientSettings: MongoClientSettings,
    collections: Tuple7F[MongoCollection, T1, T2, T3, T4, T5, T6, T7])
    : Resource[Task, Tuple7F[MongoConnector, T1, T2, T3, T4, T5, T6, T7]] =
    connection7.create(clientSettings, collections)

  /**
    * Unsafely creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *SEVEN* provided [[MongoCollection]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which will be released and closed towards the usage of the resource task.
    * Always prefer to use [[create7]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]
    */
  @UnsafeBecauseImpure
  def createUnsafe7[T1, T2, T3, T4, T5, T6, T7](
    client: MongoClient,
    collections: Tuple7F[MongoCollection, T1, T2, T3, T4, T5, T6, T7])
    : Resource[Task, Tuple7F[MongoConnector, T1, T2, T3, T4, T5, T6, T7]] =
    connection7.createUnsafe(client, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *EIGHT* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]s.
    */
  def create8[T1, T2, T3, T4, T5, T6, T7, T8](
    connectionString: String,
    collections: Tuple8F[MongoCollection, T1, T2, T3, T4, T5, T6, T7, T8])
    : Resource[Task, Tuple8F[MongoConnector, T1, T2, T3, T4, T5, T6, T7, T8]] =
    connection8.create(connectionString, collections)

  /**
    * Creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *EIGHT* provided [[MongoCollection]]s.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[MongoConnector]]s.
    * @param collections    describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]].
    */
  def create8[T1, T2, T3, T4, T5, T6, T7, T8](
    clientSettings: MongoClientSettings,
    collections: Tuple8F[MongoCollection, T1, T2, T3, T4, T5, T6, T7, T8])
    : Resource[Task, Tuple8F[MongoConnector, T1, T2, T3, T4, T5, T6, T7, T8]] =
    connection8.create(clientSettings, collections)

  /**
    * Unsafely creates a connection to mongodb and provides a [[MongoConnector]]
    * to each of the *EIGHT* provided [[MongoCollection]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which will be released and closed towards the usage of the resource task.
    * Always prefer to use [[create7]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections describes the set of collections that wants to be used (db, collectionName, codecs...)
    * @return a [[Resource]] that provides a single [[MongoConnector]] instance, linked to the specified [[MongoCollection]]
    */
  @UnsafeBecauseImpure
  def createUnsafe8[T1, T2, T3, T4, T5, T6, T7, T8](
    client: MongoClient,
    collections: Tuple8F[MongoCollection, T1, T2, T3, T4, T5, T6, T7, T8])
    : Resource[Task, Tuple8F[MongoConnector, T1, T2, T3, T4, T5, T6, T7, T8]] =
    connection8.createUnsafe(client, collections)
}

private[mongodb] trait MongoConnection[A <: Product, T2 <: Product] { self =>

  def apply(client: MongoClient, collections: A): Task[T2] = createConnectors(client, collections)

  def createConnectors(client: MongoClient, collections: A): Task[T2]

  def create(url: String, collectionInfo: A): Resource[Task, T2] =
    Resource.make(Task.now(MongoClients.create(url)))(_ => Task.unit).flatMap(createUnsafe(_, collectionInfo))

  def create(clientSettings: MongoClientSettings, collections: A): Resource[Task, T2] =
    Resource
      .make(Task.now(MongoClients.create(clientSettings)))(_ => Task.unit)
      .flatMap(createUnsafe(_, collections))

  @UnsafeBecauseImpure
  def createUnsafe(client: MongoClient, collections: A): Resource[Task, T2] = {
    Resource.make(createConnectors(client, collections))(self.close)
  }

  /** Has to be overwritten for instances of Connection where [[T2]] is
    * not a tuple/iterable [[Product]]. I.E in: [[MongoConnection.connection1]].
    */
  @UnsafeBecauseImpure
  def close(connectors: T2): Task[Unit] = {
    Observable
      .fromIterable(connectors.productIterator.toList)
      .map(_.asInstanceOf[MongoConnector[_]].close)
      .completedL
  }

}
