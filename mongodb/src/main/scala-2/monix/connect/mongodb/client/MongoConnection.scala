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

package monix.connect.mongodb.client

import cats.effect.Resource
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients, MongoDatabase}
import monix.connect.mongodb.domain._
import monix.connect.mongodb.{MongoDb, MongoSingle, MongoSink, MongoSource}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import com.mongodb.reactivestreams.client.MongoCollection
import org.mongodb.scala.ConnectionString
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY

/**
  * Singleton object that exposes signatures to create a connection to the desired
  * specified mongo collections, the abstraction to operate with collections is
  * returned in form of [[CollectionOperator]], which is based of three different
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

  private[mongodb] def connection1[Doc]: MongoConnection[Tuple1[CollectionRef[Doc]], CollectionOperator[Doc]] = {
    new MongoConnection[Tuple1[CollectionRef[Doc]], CollectionOperator[Doc]] {
      override def createCollectionOperator(
        client: MongoClient,
        collections: Tuple1[CollectionRef[Doc]]): Task[CollectionOperator[Doc]] = {
        val db: MongoDatabase = client.getDatabase(collections._1.database)
        MongoDb.createIfNotExists(db, collections._1.collection).map { _ =>
          val col: MongoCollection[Doc] = {
            collections._1 match {
              case CollectionDocumentRef(_, collectionName) => db.getCollection(collectionName)
              case codec: CollectionCodecRef[Doc] =>
                db.getCollection(collections._1.collection, codec.clazz)
                  .withCodecRegistry(fromCodecProvider(codec.codecProviders: _*))
            }
          }.asInstanceOf[MongoCollection[Doc]]
          CollectionOperator(MongoDb(client, db), MongoSource(col), MongoSingle(col), MongoSink(col))
        }
      }
    }
  }

  private[mongodb] def connection2[T1, T2]
    : MongoConnection[Tuple2F[CollectionRef, T1, T2], Tuple2F[CollectionOperator, T1, T2]] =
    (client: MongoClient, collections: Tuple2F[CollectionRef, T1, T2]) => {
      for {
        one <- connection1[T1](client, Tuple1(collections._1))
        two <- connection1[T2](client, Tuple1(collections._2))
      } yield (one, two)
    }

  private[mongodb] def connection3[T1, T2, T3]
    : MongoConnection[Tuple3F[CollectionRef, T1, T2, T3], Tuple3F[CollectionOperator, T1, T2, T3]] =
    (client: MongoClient, collections: Tuple3F[CollectionRef, T1, T2, T3]) => {
      val (c1, c2, c3) = collections
      for {
        a <- connection1[T1](client, Tuple1(c1))
        b <- connection2[T2, T3].createCollectionOperator(client, (c2, c3))
      } yield (a, b._1, b._2)
    }

  private[mongodb] def connection4[T1, T2, T3, T4]
    : MongoConnection[Tuple4F[CollectionRef, T1, T2, T3, T4], Tuple4F[CollectionOperator, T1, T2, T3, T4]] =
    (client: MongoClient, collections: Tuple4F[CollectionRef, T1, T2, T3, T4]) => {
      val (c1, c2, c3, c4) = collections
      for {
        a <- connection1[T1](client, Tuple1(c1))
        b <- connection3[T2, T3, T4].createCollectionOperator(client, (c2, c3, c4))
      } yield (a, b._1, b._2, b._3)
    }

  private[mongodb] def connection5[T1, T2, T3, T4, T5]
    : MongoConnection[Tuple5F[CollectionRef, T1, T2, T3, T4, T5], Tuple5F[CollectionOperator, T1, T2, T3, T4, T5]] =
    (client: MongoClient, collections: Tuple5F[CollectionRef, T1, T2, T3, T4, T5]) => {
      val (c1, c2, c3, c4, c5) = collections

      for {
        a <- connection1[T1](client, Tuple1(c1))
        b <- connection4[T2, T3, T4, T5].createCollectionOperator(client, (c2, c3, c4, c5))
      } yield (a, b._1, b._2, b._3, b._4)
    }

  private[mongodb] def connection6[T1, T2, T3, T4, T5, T6]: MongoConnection[
    Tuple6F[CollectionRef, T1, T2, T3, T4, T5, T6],
    Tuple6F[CollectionOperator, T1, T2, T3, T4, T5, T6]] =
    (client: MongoClient, collections: Tuple6F[CollectionRef, T1, T2, T3, T4, T5, T6]) => {
      val (c1, c2, c3, c4, c5, c6) = collections
      for {
        a <- connection1[T1](client, Tuple1(c1))
        b <- connection5[T2, T3, T4, T5, T6].createCollectionOperator(client, (c2, c3, c4, c5, c6))
      } yield (a, b._1, b._2, b._3, b._4, b._5)
    }

  private[mongodb] def connection7[T1, T2, T3, T4, T5, T6, T7]: MongoConnection[
    Tuple7F[CollectionRef, T1, T2, T3, T4, T5, T6, T7],
    Tuple7F[CollectionOperator, T1, T2, T3, T4, T5, T6, T7]] =
    (client: MongoClient, collections: Tuple7F[CollectionRef, T1, T2, T3, T4, T5, T6, T7]) => {
      val (c1, c2, c3, c4, c5, c6, c7) = collections
      for {
        a <- connection1[T1](client, Tuple1(c1))
        b <- connection6[T2, T3, T4, T5, T6, T7].createCollectionOperator(client, (c2, c3, c4, c5, c6, c7))
      } yield (a, b._1, b._2, b._3, b._4, b._5, b._6)
    }

  private[mongodb] def connection8[T1, T2, T3, T4, T5, T6, T7, T8]: MongoConnection[
    Tuple8F[CollectionRef, T1, T2, T3, T4, T5, T6, T7, T8],
    Tuple8F[CollectionOperator, T1, T2, T3, T4, T5, T6, T7, T8]] =
    (client: MongoClient, collections: Tuple8F[CollectionRef, T1, T2, T3, T4, T5, T6, T7, T8]) => {
      val (c1, c2, c3, c4, c5, c6, c7, c8) = collections
      for {
        a <- connection1[T1](client, Tuple1(c1))
        b <- connection7[T2, T3, T4, T5, T6, T7, T8].createCollectionOperator(client, (c2, c3, c4, c5, c6, c7, c8))
      } yield (a, b._1, b._2, b._3, b._4, b._5, b._6, b._7)
    }

  /**
    * Creates a resource-safe mongodb connection that encapsulates a
    * [[CollectionOperator]] to interoperate with the given [[CollectionRef]].
    *
    * ==Example==
    * {{{
    *   import com.mongodb.client.model.Filters
    *   import monix.eval.Task
    *   import monix.connect.mongodb.client.{MongoConnection, CollectionOperator, CollectionCodecRef}
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *
    *   case class Employee(name: String, age: Int, companyName: String = "X")
    *
    *   val employee = Employee("Stephen", 32)
    *   val employeesCol = CollectionCodecRef("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
    *   val connection = MongoConnection.create1("mongodb://localhost:27017", employeesCol)
    *
    *   val t: Task[Employee] =
    *   connection.use { case CollectionOperator(db, source, single, sink) =>
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
    * @param collection    the collection reference which we want to interoperate with,
    *                      represented [[CollectionRef]].

    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create1[T1](connectionString: String, collection: CollectionRef[T1]): Resource[Task, CollectionOperator[T1]] =
    connection1[T1].create(connectionString, Tuple1(collection))

  /**
    * Creates a resource-safe mongodb connection, that encapsulates a
    * [[CollectionOperator]] to interoperate with the given [[CollectionRef]]s.
    *
    * ==Example==
    * {{{
    *   import com.mongodb.client.model.Filters
    *   import monix.eval.Task
    *   import com.mongodb.{MongoClientSettings, ServerAddress}
    *   import monix.connect.mongodb.client.{MongoConnection, CollectionOperator, CollectionCodecRef}
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *
    *   import scala.jdk.CollectionConverters._
    *
    *   case class Employee(name: String, age: Int, companyName: String = "X")
    *
    *   val employee = Employee("Stephen", 32)
    *   val employeesCol = CollectionCodecRef("myDb", "employees", classOf[Employee], createCodecProvider[Employee]())
    *
    *   val mongoClientSettings = MongoClientSettings.builder
    *       .applyToClusterSettings(builder => builder.hosts(List(new ServerAddress("localhost", 27017)).asJava))
    *       .build
    *
    *   val connection = MongoConnection.create1(mongoClientSettings, employeesCol)
    *   val t: Task[Employee] =
    *   connection.use { case CollectionOperator(db, source, single, sink) =>
    *     // business logic here
    *     single.insertOne(employee)
    *       .flatMap(_ => source.find(Filters.eq("name", employee.name)).headL)
    *   }
    * }}}
    *
    * @param clientSettings various settings to control the behavior the created [[CollectionOperator]].
    * @param collection   the collection reference which we want to interoperate with,
    *                       represented as [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create1[T1](
    clientSettings: MongoClientSettings,
    collection: CollectionRef[T1]): Resource[Task, CollectionOperator[T1]] =
    connection1[T1].create(clientSettings, Tuple1(collection))

  /**
    * Unsafely creates mongodb connection that provides a [[CollectionOperator]]
    * to interoperate with the respective [[CollectionRef]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed.
    * The connection resources are not released after its usage, you
    * could use `guarantee` or `bracket` to perform the same action.
    *
    * Always prefer to use [[create1]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collection   the collection reference which we want to interoperate with,
    *                     represented as [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]
    */
  @UnsafeBecauseImpure
  def createUnsafe1[T1](client: MongoClient, collection: CollectionRef[T1]): Task[CollectionOperator[T1]] =
    connection1[T1].createUnsafe(client, Tuple1(collection))

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple2]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * ==Example==
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.mongodb.client.{MongoConnection, CollectionOperator, CollectionCodecRef}
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *
    *   case class Employee(name: String, age: Int, companyName: String = "X")
    *   case class Company(name: String, employees: List[Employee], investment: Int = 0)
    *
    *   val employee1 = Employee("Gerard", 39)
    *   val employee2 = Employee("Laura", 41)
    *   val company = Company("Stephen", List(employee1, employee2))
    *
    *   val employeesCol = CollectionCodecRef("business", "employees_collection", classOf[Employee], createCodecProvider[Employee]())
    *   val companiesCol = CollectionCodecRef("business", "companies_collection", classOf[Company], createCodecProvider[Company](), createCodecProvider[Employee]())
    *
    *   val connection = MongoConnection.create2("mongodb://localhost:27017", (employeesCol, companiesCol))
    *
    *   val t: Task[Unit] =
    *   connection.use { case (CollectionOperator(_, employeeSource, employeeSingle, employeeSink),
    *                          CollectionOperator(_, companySource, companySingle, companySink)) =>
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
    * @param collections the two collection references which we want to interoperate with,
    *                    represented as [[Tuple2]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]s.
    */
  def create2[T1, T2](
    connectionString: String,
    collections: Tuple2F[CollectionRef, T1, T2]): Resource[Task, Tuple2F[CollectionOperator, T1, T2]] =
    connection2.create(connectionString, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple2]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s, given in the `collections` parameter.
    *
    * ==Example==
    * {{{
    *   import monix.eval.Task
    *   import monix.connect.mongodb.client.{MongoConnection, CollectionOperator, CollectionCodecRef}
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
    *   val employeesCol = CollectionCodecRef("business", "employees_collection", classOf[Employee], createCodecProvider[Employee]())
    *   val companiesCol = CollectionCodecRef("business", "companies_collection", classOf[Company], createCodecProvider[Company](), createCodecProvider[Employee]())
    *
    *   val mongoClientSettings = MongoClientSettings.builder
    *       .applyToClusterSettings(builder => builder.hosts(List(new ServerAddress("localhost", 27017)).asJava))
    *       .build
    *
    *   val connection = MongoConnection.create2(mongoClientSettings, (employeesCol, companiesCol))
    *
    *   val t: Task[Unit] =
    *   connection.use { case (CollectionOperator(_, employeeSource, employeeSingle, employeeSink),
    *                          CollectionOperator(_, companySource, companySingle, companySink)) =>
    *     // business logic here
    *     for {
    *       r1 <- employeeSingle.insertMany(List(employee1, employee2))
    *       r2 <- companySingle.insertOne(company)
    *     } yield ()
    *   }
    * }}}
    *
    * @param clientSettings various settings to control the behavior of the created [[CollectionOperator]]s.
    * @param collections    the two collection references which we want to interoperate with
    *                       represented as [[Tuple2]] of the [[CollectionRef]]
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create2[T1, T2](
    clientSettings: MongoClientSettings,
    collections: Tuple2F[CollectionRef, T1, T2]): Resource[Task, Tuple2F[CollectionOperator, T1, T2]] =
    connection2.create(clientSettings, collections)

  /**
    * Unsafely creates mongodb connection that provides a [[Tuple2]] of [[CollectionOperator]]
    * to interoperate with the respective [[CollectionRef]]s, given in the `collections` parameter.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed.
    * The connection resources are not released after its usage, you
    * could use `guarantee` or `bracket` to perform the same action.
    * Always prefer to use [[create2]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections    the three collection references which we want to interoperate with,
    *                       represented as [[Tuple3]] of the [[CollectionRef]]
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]
    */
  @UnsafeBecauseImpure
  def createUnsafe2[T1, T2](
    client: MongoClient,
    collections: Tuple2F[CollectionRef, T1, T2]): Task[Tuple2F[CollectionOperator, T1, T2]] =
    connection2.createUnsafe(client, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple3]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s, given in the `collections` parameter.
    *
    * ==Example==
    * {{{
    *   import com.mongodb.client.model.{Filters, Updates}
    *   import monix.eval.Task
    *   import monix.connect.mongodb.client.{MongoConnection, CollectionCodecRef}
    *   import monix.connect.mongodb.domain.UpdateResult
    *   import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
    *
    *   import scala.concurrent.duration._
    *   import scala.jdk.CollectionConverters._
    *
    *   case class Employee(name: String, age: Int, companyName: String)
    *   case class Company(name: String, employees: List[Employee], investment: Int = 0)
    *   case class Investor(name: String, funds: Int, companies: List[Company])
    *
    *   val companiesCol = CollectionCodecRef(
    *         "my_db",
    *         "companies_collection",
    *         classOf[Company],
    *         createCodecProvider[Company](),
    *         createCodecProvider[Employee]())
    *   val employeesCol =
    *     CollectionCodecRef("my_db", "employees_collection", classOf[Employee], createCodecProvider[Employee]())
    *   val investorsCol = CollectionCodecRef(
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
    * @param collections    the three collection references which we want to interoperate with,
    *                       represented as [[Tuple3]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]s.
    */
  def create3[T1, T2, T3](
    connectionString: String,
    collections: Tuple3F[CollectionRef, T1, T2, T3]): Resource[Task, Tuple3F[CollectionOperator, T1, T2, T3]] =
    connection3.create(connectionString, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple3]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @param clientSettings various settings to control the behavior of the created [[CollectionOperator]]s.
    * @param collections    the three collection references which we want to interoperate with
    *                       represented as [[Tuple3]] of the [[CollectionRef]]
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create3[T1, T2, T3](
    clientSettings: MongoClientSettings,
    collections: Tuple3F[CollectionRef, T1, T2, T3]): Resource[Task, Tuple3F[CollectionOperator, T1, T2, T3]] =
    connection3.create(clientSettings, collections)

  /**
    * Unsafely creates mongodb connection that provides a [[Tuple8[CollectionOperator] ]]
    * to interoperate with the respective [[CollectionRef]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed.
    * The connection resources are not released after its usage, you
    * could use `guarantee` or `bracket` to perform the same action.
    * Always prefer to use [[create3]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections    the three collection references which we want to interoperate with,
    *                       represented as [[Tuple3]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]
    */
  @UnsafeBecauseImpure
  def createUnsafe3[T1, T2, T3](
    client: MongoClient,
    collections: Tuple3F[CollectionRef, T1, T2, T3]): Task[Tuple3F[CollectionOperator, T1, T2, T3]] =
    connection3.createUnsafe(client, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple4]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information on how to configure it
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections the four collection references which we want to interoperate with,
    *                    represented as [[Tuple4]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]s.
    */
  def create4[T1, T2, T3, T4](
    connectionString: String,
    collections: Tuple4F[CollectionRef, T1, T2, T3, T4]): Resource[Task, Tuple4F[CollectionOperator, T1, T2, T3, T4]] =
    connection4.create(connectionString, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple4]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[CollectionOperator]]s.
    * @param collections    the four collection references which we want to interoperate with,
    *                       represented as [[Tuple4]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create4[T1, T2, T3, T4](
    clientSettings: MongoClientSettings,
    collections: Tuple4F[CollectionRef, T1, T2, T3, T4]): Resource[Task, Tuple4F[CollectionOperator, T1, T2, T3, T4]] =
    connection4.create(clientSettings, collections)

  /**
    * Unsafely creates mongodb connection that provides a [[Tuple4[CollectionOperator] ]]
    * to interoperate with the respective [[CollectionRef]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed.
    * The connection resources are not released after its usage, you
    * could use `guarantee` or `bracket` to perform the same action.
    * Always prefer to use [[create4]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections    the four collection references which we want to interoperate with,
    *                       represented as [[Tuple4]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]
    */
  @UnsafeBecauseImpure
  def createUnsafe4[T1, T2, T3, T4](
    client: MongoClient,
    collections: Tuple4F[CollectionRef, T1, T2, T3, T4]): Task[Tuple4F[CollectionOperator, T1, T2, T3, T4]] =
    connection4.createUnsafe(client, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple5]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections    the five collection references which we want to interoperate with,
    *                       represented as [[Tuple5]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]s.
    */
  def create5[T1, T2, T3, T4, T5](connectionString: String, collections: Tuple5F[CollectionRef, T1, T2, T3, T4, T5])
    : Resource[Task, Tuple5F[CollectionOperator, T1, T2, T3, T4, T5]] =
    connection5.create(connectionString, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple5]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[CollectionOperator]]s.
    * @param collections    the five collection references which we want to interoperate with,
    *                       represented as [[Tuple5]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create5[T1, T2, T3, T4, T5](
    clientSettings: MongoClientSettings,
    collections: Tuple5F[CollectionRef, T1, T2, T3, T4, T5])
    : Resource[Task, Tuple5F[CollectionOperator, T1, T2, T3, T4, T5]] =
    connection5.create(clientSettings, collections)

  /**
    * Unsafely creates mongodb connection that provides a [[Tuple5[CollectionOperator] ]]
    * to interoperate with the respective [[CollectionRef]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed.
    * The connection resources are not released after its usage, you
    * could use `guarantee` or `bracket` to perform the same action.
    * Always prefer to use [[create5]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections    the five collection references which we want to interoperate with,
    *                       represented as [[Tuple5]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]
    */
  @UnsafeBecauseImpure
  def createUnsafe5[T1, T2, T3, T4, T5](
    client: MongoClient,
    collections: Tuple5F[CollectionRef, T1, T2, T3, T4, T5]): Task[Tuple5F[CollectionOperator, T1, T2, T3, T4, T5]] =
    connection5.createUnsafe(client, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple6]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections    the six collection references which we want to interoperate with,
    *                       represented as [[Tuple5]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]s.
    */
  def create6[T1, T2, T3, T4, T5, T6](
    connectionString: String,
    collections: Tuple6F[CollectionRef, T1, T2, T3, T4, T5, T6])
    : Resource[Task, Tuple6F[CollectionOperator, T1, T2, T3, T4, T5, T6]] =
    connection6.create(connectionString, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple6]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[CollectionOperator]]s.
    * @param collections    the six collection references which we want to interoperate with,
    *                       represented as [[Tuple5]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create6[T1, T2, T3, T4, T5, T6](
    clientSettings: MongoClientSettings,
    collections: Tuple6F[CollectionRef, T1, T2, T3, T4, T5, T6])
    : Resource[Task, Tuple6F[CollectionOperator, T1, T2, T3, T4, T5, T6]] =
    connection6.create(clientSettings, collections)

  /**
    * Unsafely creates mongodb connection that provides a [[Tuple6[CollectionOperator] ]]
    * to interoperate with the respective [[CollectionRef]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed.
    * The connection resources are not released after its usage, you
    * could use `guarantee` or `bracket` to perform the same action.
    * Always prefer to use [[create6]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections    the six collection references which we want to interoperate with,
    *                       represented as [[Tuple6]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]
    */
  def createUnsafe6[T1, T2, T3, T4, T5, T6](
    client: MongoClient,
    collections: Tuple6F[CollectionRef, T1, T2, T3, T4, T5, T6])
    : Task[Tuple6F[CollectionOperator, T1, T2, T3, T4, T5, T6]] =
    connection6.createUnsafe(client, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple7]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections    the seven collection references which we want to interoperate with,
    *                       represented as [[Tuple6]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]s.
    */
  def create7[T1, T2, T3, T4, T5, T6, T7](
    connectionString: String,
    collections: Tuple7F[CollectionRef, T1, T2, T3, T4, T5, T6, T7])
    : Resource[Task, Tuple7F[CollectionOperator, T1, T2, T3, T4, T5, T6, T7]] =
    connection7.create(connectionString, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple7]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[CollectionOperator]]s.
    * @param collections    the seven collection references which we want to interoperate with,
    *                       represented as [[Tuple7]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create7[T1, T2, T3, T4, T5, T6, T7](
    clientSettings: MongoClientSettings,
    collections: Tuple7F[CollectionRef, T1, T2, T3, T4, T5, T6, T7])
    : Resource[Task, Tuple7F[CollectionOperator, T1, T2, T3, T4, T5, T6, T7]] =
    connection7.create(clientSettings, collections)

  /**
    * Unsafely creates mongodb connection that provides a [[Tuple7]] of [[CollectionOperator]]
    * to interoperate with the respective [[CollectionRef]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed.
    * The connection resources are not released after its usage, you
    * could use `guarantee` or `bracket` to perform the same action.
    * Always prefer to use [[create7]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections    the seven collection references which we want to interoperate with,
    *                       represented as [[Tuple7]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]
    */
  @UnsafeBecauseImpure
  def createUnsafe7[T1, T2, T3, T4, T5, T6, T7](
    client: MongoClient,
    collections: Tuple7F[CollectionRef, T1, T2, T3, T4, T5, T6, T7])
    : Task[Tuple7F[CollectionOperator, T1, T2, T3, T4, T5, T6, T7]] =
    connection7.createUnsafe(client, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple8]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param connectionString describes the hosts, ports and options to be used.
    *                         @see for more information to configure the connection string
    *                         [[https://mongodb.github.io/mongo-java-driver/3.9/javadoc/com/mongodb/ConnectionString.html]]
    *                         and [[https://mongodb.github.io/mongo-java-driver/3.7/driver/tutorials/connect-to-mongodb/]]
    * @param collections the eight collection references which we want to interoperate with,
    *                    represented as [[Tuple8]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]s.
    */
  def create8[T1, T2, T3, T4, T5, T6, T7, T8](
    connectionString: String,
    collections: Tuple8F[CollectionRef, T1, T2, T3, T4, T5, T6, T7, T8])
    : Resource[Task, Tuple8F[CollectionOperator, T1, T2, T3, T4, T5, T6, T7, T8]] =
    connection8.create(connectionString, collections)

  /**
    * Creates a resource-safe mongodb connection that encapsulates a [[Tuple8]] of [[CollectionOperator]]
    * to interoperate with its relative [[CollectionRef]]s given in the `collections` parameter.
    *
    * @see an example of usage could be extrapolated from the scaladoc
    *      example of [[create1]], [[create2]] and [[create3]].
    * @param clientSettings various settings to control the behavior of the created [[CollectionOperator]]s.
    * @param collections the eight collection references which we want to interoperate with,
    *                    represented as [[Tuple8]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]].
    */
  def create8[T1, T2, T3, T4, T5, T6, T7, T8](
    clientSettings: MongoClientSettings,
    collections: Tuple8F[CollectionRef, T1, T2, T3, T4, T5, T6, T7, T8])
    : Resource[Task, Tuple8F[CollectionOperator, T1, T2, T3, T4, T5, T6, T7, T8]] =
    connection8.create(clientSettings, collections)

  /**
    * Unsafely creates mongodb connection that provides a [[Tuple8]] of [[CollectionOperator]]
    * to interoperate with the respective [[CollectionRef]]s.
    *
    * WARN: It is unsafe because it directly expects an instance of [[MongoClient]],
    * which might have already been closed.
    * The connection resources are not released after its usage, you
    * could use `guarantee` or `bracket` to perform the same action.
    * Always prefer to use [[create7]].
    *
    * @param client an instance of [[MongoClient]]
    * @param collections the eight collection references which we want to interoperate with,
    *                    represented as [[Tuple8]] of [[CollectionRef]].
    * @return a [[Resource]] that provides a single [[CollectionOperator]] instance, linked to the specified [[CollectionRef]]
    */
  @UnsafeBecauseImpure
  def createUnsafe8[T1, T2, T3, T4, T5, T6, T7, T8](
    client: MongoClient,
    collections: Tuple8F[CollectionRef, T1, T2, T3, T4, T5, T6, T7, T8])
    : Task[Tuple8F[CollectionOperator, T1, T2, T3, T4, T5, T6, T7, T8]] =
    connection8.createUnsafe(client, collections)
}

private[mongodb] trait MongoConnection[A <: Product, T2 <: Product] { self =>

  def apply(client: MongoClient, collections: A): Task[T2] = createCollectionOperator(client, collections)

  def createCollectionOperator(client: MongoClient, collections: A): Task[T2]

  def create(connectionStr: String, collectionRefs: A): Resource[Task, T2] =
    for {
      client             <- Resource.fromAutoCloseable(Task.evalAsync(MongoClients.create(connectionStr)))
      collectionOperator <- Resource.liftF(createCollectionOperator(client, collectionRefs))
    } yield collectionOperator

  def create(connectionStr: ConnectionString, collectionRefs: A): Resource[Task, T2] =
    for {
      client             <- Resource.fromAutoCloseable(Task.evalAsync(MongoClients.create(connectionStr)))
      collectionOperator <- Resource.liftF(createCollectionOperator(client, collectionRefs))
    } yield collectionOperator

  def create(clientSettings: MongoClientSettings, collectionRefs: A): Resource[Task, T2] =
    for {
      client             <- Resource.fromAutoCloseable(Task.evalAsync(MongoClients.create(clientSettings)))
      collectionOperator <- Resource.liftF(createCollectionOperator(client, collectionRefs))
    } yield collectionOperator

  @UnsafeBecauseImpure
  def createUnsafe(client: MongoClient, collectionRefs: A): Task[T2] = {
    for {
      client             <- Task.evalAsync(client)
      collectionOperator <- createCollectionOperator(client, collectionRefs)
    } yield collectionOperator
  }

}
