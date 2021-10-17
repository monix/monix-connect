/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://monix.io
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
import com.mongodb.client.model.Filters
import com.mongodb.reactivestreams.client.MongoClients
import monix.connect.mongodb.client.{CollectionCodecRef, CollectionOperator, CollectionRef, MongoConnection}
import monix.connect.mongodb.domain.{Tuple4F, Tuple5F, Tuple6F, Tuple7F, Tuple8F}
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers


class MongoConnectionSuite extends AsyncFlatSpec with MonixTaskSpec with Matchers with BeforeAndAfterEach with MongoConnectionFixture {

  override implicit val scheduler: Scheduler = Scheduler.io("mongo-connection-suite")

  override def beforeEach() = {
    super.beforeEach()
    MongoDb.dropDatabase(db).runSyncUnsafe()
    MongoDb.dropCollection(db, employeesColName).runSyncUnsafe()
    MongoDb.dropCollection(db, companiesColName).runSyncUnsafe()
    MongoDb.dropCollection(db, investorsColName).runSyncUnsafe()
  }

  "A single collection" should "be created given the url endpoint" in {
    val collectionName = Gen.identifier.sample.get
    val investor = genInvestor.sample.get
    val connection = MongoConnection
      .create1(
        mongoEndpoint,
        CollectionCodecRef(
          dbName,
          collectionName,
          classOf[Investor],
          createCodecProvider[Employee](),
          createCodecProvider[Company](),
          createCodecProvider[Investor]())
      )

    val r = connection.use {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(investor).flatMap(_ => source.find(Filters.eq("name", investor.name)).headL)
    }.runSyncUnsafe()

    r shouldBe investor
  }

  it should "be created given the mongo client settings" in {
    val collectionName = Gen.identifier.sample.get
    val employee = genEmployee.sample.get

    val connection = MongoConnection.create1(
      mongoClientSettings,
      CollectionCodecRef(dbName, collectionName, classOf[Employee], createCodecProvider[Employee]()))

    connection.use {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(employee).flatMap(_ => source.find(Filters.eq("name", employee.name)).headL)
    }.asserting(_ shouldBe employee)
  }

  it should "be created unsafely given a mongo client" in {
    val collectionName = Gen.identifier.sample.get
    val employee = genEmployee.sample.get
    val col = CollectionCodecRef(dbName, collectionName, classOf[Employee], createCodecProvider[Employee]())
    val connection = MongoConnection.createUnsafe1(MongoClients.create(mongoEndpoint), col)

    connection.flatMap {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(employee).flatMap(_ => source.find(Filters.eq("name", employee.name)).headL)
    }.asserting(_ shouldBe employee)
  }

  "Two collections" should "be created given the url endpoint" in {
    def makeResource(col1: CollectionRef[Employee], col2: CollectionRef[Company]) =
      MongoConnection.create2(mongoEndpoint, (col1, col2))

    createConnectionTest2(makeResource)
  }

  it should "be created given the mongo client settings" in {
    def makeResource(col1: CollectionRef[Employee], col2: CollectionRef[Company]) =
      MongoConnection.create2(mongoClientSettings, (col1, col2))

    createConnectionTest2(makeResource)
  }

  it should "be created unsafely given the mongo client" in {
    def makeResource(col1: CollectionRef[Employee], col2: CollectionRef[Company]) =
      Resource.liftF(MongoConnection.createUnsafe2(MongoClients.create(mongoEndpoint), (col1, col2)))

    createConnectionTest2(makeResource)
  }

  "Three collections" should "be created given the url endpoint" in {
    def makeResource(col1: CollectionRef[Company], col2: CollectionRef[Employee], col3: CollectionRef[Investor]) =
      MongoConnection.create3(mongoEndpoint, (col1, col2, col3))

    abstractCreateConnectionTest3(makeResource)
  }

  it should "be created given the mongo client settings" in {
    def makeResource(col1: CollectionRef[Company], col2: CollectionRef[Employee], col3: CollectionRef[Investor]) =
      MongoConnection.create3(mongoClientSettings, (col1, col2, col3))

    abstractCreateConnectionTest3(makeResource)
  }

  it should "be created unsafely given a mongo client" in {
    def makeResource(col1: CollectionRef[Company], col2: CollectionRef[Employee], col3: CollectionRef[Investor]) =
      Resource.liftF(MongoConnection.createUnsafe3(MongoClients.create(mongoEndpoint), (col1, col2, col3)))

    abstractCreateConnectionTest3(makeResource)
  }

  "Four collections" should "be created given the url endpoint" in {
    val makeResource = (collections: Tuple4F[CollectionRef, Employee, Employee, Employee, Company]) =>
      MongoConnection.create4(mongoEndpoint, (collections._1, collections._2, collections._3, collections._4))
    abstractCreateConnectionTest4(makeResource)
  }

  it should "be created given the mongo client settings" in {
    val makeResource = (collections: Tuple4F[CollectionRef, Employee, Employee, Employee, Company]) =>
      MongoConnection.create4(mongoClientSettings, (collections._1, collections._2, collections._3, collections._4))
    abstractCreateConnectionTest4(makeResource)
  }

  it should "be created unsafely given a mongo client" in {
    val makeResource = (collections: Tuple4F[CollectionRef, Employee, Employee, Employee, Company]) =>
      Resource.liftF(
        MongoConnection.createUnsafe4(
          MongoClients.create(mongoEndpoint),
          (collections._1, collections._2, collections._3, collections._4))
      )
    abstractCreateConnectionTest4(makeResource)
  }

  "Five collections" should "be created given the url endpoint" in {
    val makeResource = (collections: Tuple5F[CollectionRef, Employee, Employee, Employee, Employee, Company]) =>
      MongoConnection
        .create5(mongoEndpoint, (collections._1, collections._2, collections._3, collections._4, collections._5))
    abstractCreateConnectionTest5(makeResource)
  }

  it should "be created given the mongo client settings" in {
    val makeResource = (collections: Tuple5F[CollectionRef, Employee, Employee, Employee, Employee, Company]) =>
      MongoConnection
        .create5(mongoClientSettings, (collections._1, collections._2, collections._3, collections._4, collections._5))
    abstractCreateConnectionTest5(makeResource)
  }

  it should "be created unsafely given a mongo client" in {
    val makeResource = (collections: Tuple5F[CollectionRef, Employee, Employee, Employee, Employee, Company]) =>
      Resource.liftF(
        MongoConnection.createUnsafe5(
          MongoClients.create(mongoEndpoint),
          (collections._1, collections._2, collections._3, collections._4, collections._5)
        )
      )
    abstractCreateConnectionTest5(makeResource)
  }

  "Six collections" should "be created given the url endpoint" in {
    val makeResource =
      (collections: Tuple6F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6) = collections
        MongoConnection
          .create6(mongoEndpoint, (c1, c2, c3, c4, c5, c6))
      }
    abstractCreateConnectionTest6(makeResource)
  }

  it should "be created given the mongo client settings" in {
    val makeResource =
      (collections: Tuple6F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6) = collections
        MongoConnection
          .create6(mongoClientSettings, (c1, c2, c3, c4, c5, c6))
      }
    abstractCreateConnectionTest6(makeResource)
  }

  it should "be created unsafely given a mongo client" in {
    val makeResource =
      (collections: Tuple6F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6) = collections
        Resource.liftF(
          MongoConnection
            .createUnsafe6(MongoClients.create(mongoEndpoint), (c1, c2, c3, c4, c5, c6))
        )
      }
    abstractCreateConnectionTest6(makeResource)
  }

  "Seven collections" should "be created given the url endpoint" in {
    val makeResource =
      (collections: Tuple7F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6, c7) = collections
        MongoConnection
          .create7(mongoEndpoint, (c1, c2, c3, c4, c5, c6, c7))
      }
    abstractCreateConnectionTest7(makeResource)
  }

  it should "be created given the mongo client settings" in {
    val makeResource =
      (collections: Tuple7F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6, c7) = collections
        MongoConnection
          .create7(mongoClientSettings, (c1, c2, c3, c4, c5, c6, c7))
      }
    abstractCreateConnectionTest7(makeResource)
  }

  it should "be created unsafely given a mongo client" in {
    val makeResource =
      (collections: Tuple7F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6, c7) = collections
        Resource.liftF(
          MongoConnection
            .createUnsafe7(MongoClients.create(mongoEndpoint), (c1, c2, c3, c4, c5, c6, c7))
        )
      }
    abstractCreateConnectionTest7(makeResource)
  }

  "Eight collections" should "be created given the url endpoint" in {
    val makeResource =
      (collections: Tuple8F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6, c7, c8) = collections
        MongoConnection
          .create8(mongoEndpoint, (c1, c2, c3, c4, c5, c6, c7, c8))
      }
    abstractCreateConnectionTest8(makeResource)
  }

  it should "be created given the mongo client settings" in {
    val makeResource =
      (collections: Tuple8F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6, c7, c8) = collections
        MongoConnection
          .create8(mongoClientSettings, (c1, c2, c3, c4, c5, c6, c7, c8))
      }
    abstractCreateConnectionTest8(makeResource)
  }

  it should "be created unsafely given a mongo client" in {
    val makeResource =
      (collections: Tuple8F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Employee, Employee, Company]) => {
        val (c1, c2, c3, c4, c5, c6, c7, c8) = collections
        Resource.liftF(
          MongoConnection
            .createUnsafe8(MongoClients.create(mongoEndpoint), (c1, c2, c3, c4, c5, c6, c7, c8))
        )
      }
    abstractCreateConnectionTest8(makeResource)
  }

}
