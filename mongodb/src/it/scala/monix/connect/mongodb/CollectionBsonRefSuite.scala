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
import com.mongodb.{MongoClientSettings, ServerAddress}
import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.reactivestreams.client.MongoClients
import monix.connect.mongodb.client.{CollectionBsonRef, CollectionCodec, CollectionOperator, CollectionRef, MongoConnection}
import monix.connect.mongodb.domain.{Tuple2F, Tuple3F, Tuple4F, Tuple5F, Tuple6F, Tuple7F, Tuple8F}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.bson.Document
import org.bson.conversions.Bson
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class CollectionBsonRefSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {

  override def beforeEach() = {
    super.beforeEach()
    MongoDb.dropDatabase(db).runSyncUnsafe()
    MongoDb.dropCollection(db, employeesColName).runSyncUnsafe()
    MongoDb.dropCollection(db, companiesColName).runSyncUnsafe()
    MongoDb.dropCollection(db, investorsColName).runSyncUnsafe()
  }

  "A single bson collection" should "be created given the url endpoint" in {
    //given
    val collectionName = Gen.identifier.sample.get
    val name = "Alice"
    val age = 22
    val person = Document.parse(s"""{"name":"$name", "age":$age }""")

    val connection = MongoConnection
      .create1(
        mongoEndpoint,
        CollectionBsonRef(
          dbName,
          collectionName)
      )

    //when
    val r = connection.use {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(person).flatMap(_ => source.find(Filters.eq("name", name)).headL)
    }.runSyncUnsafe()

    //then
    r shouldBe person
  }

  it should "be created given the mongo client settings" in new MongoConnectionFixture {
    //given
    val collectionName: String = Gen.identifier.sample.get
    val name = "Margaret"
    val age = 54
    val margaret: Document = Document.parse(s"""{"name":"$name", "age":$age }""")

    val bsonConnection: Resource[Task, CollectionOperator[Bson]] = MongoConnection.create1(
      mongoClientSettings,
      CollectionBsonRef(dbName, collectionName))

    //when
    val r = bsonConnection.use {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(margaret).flatMap(_ => source.find(Filters.eq("name", name)).headL)
    }.runSyncUnsafe()

    //then
    r shouldBe margaret
  }

  it should "be created unsafely given a mongo client" in {
    //given
    val collectionName: String = Gen.identifier.sample.get
    val name = "Greg"
    val age = 41
    val greg: Document = Document.parse(s"""{"name":"$name", "age":$age }""")
    val coll = CollectionBsonRef(
      dbName,
      collectionName)
    val connection = MongoConnection.createUnsafe1(MongoClients.create(mongoEndpoint), coll)

    //when
    val r = connection.use {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(greg).flatMap(_ => source.find(Filters.eq("name", name)).headL)
    }.runSyncUnsafe()

    //then
    r shouldBe greg
  }

//"Two collections" should "be created given the url endpoint" in new MongoConnectionFixture {
//  def makeResource(col1: CollectionRef[Employee], col2: CollectionRef[Company]) =
//    MongoConnection.create2(mongoEndpoint, (col1, col2))
//  createConnectionTest2(makeResource)
//}

//"Three collections" should "be created given the url endpoint" in new MongoConnectionFixture {
//  def makeResource(col1: CollectionRef[Company], col2: CollectionRef[Employee], col3: CollectionRef[Investor]) =
//    MongoConnection.create3(mongoEndpoint, (col1, col2, col3))
//  abstractCreateConnectionTest3(makeResource)
//}

//"Three collections" should "be created given the url endpoint" in new MongoConnectionFixture {
//  def makeResource(col1: CollectionRef[Company], col2: CollectionRef[Employee], col3: CollectionRef[Investor]) =
//    MongoConnection.create3(mongoEndpoint, (col1, col2, col3))
//  abstractCreateConnectionTest3(makeResource)
//}



  trait MongoConnectionFixture {

    val mongoClientSettings =
      MongoClientSettings
        .builder()
        .applyToClusterSettings(builder => builder.hosts(List(new ServerAddress("localhost", 27017)).asJava))
        .build()

    protected[this] def createConnectionTest2(
                                               makeResource: (
                                                 CollectionRef[Employee],
                                                   CollectionRef[Company]) => Resource[Task, Tuple2F[CollectionOperator, Employee, Company]]): Assertion = {
      //given
      val employee = genEmployee.sample.get
      val company = genCompany.sample.get
      val connection = makeResource(employeesCol, companiesCol)

      //when
      val (r1, r2) = connection.use {
        case (
          CollectionOperator(employeeDb, employeeSource, employeeSingle, employeeSink),
          CollectionOperator(companyDb, companySource, companySingle, companySink)) =>
          val r1 = employeeSingle
            .insertOne(employee)
            .flatMap(_ => employeeSource.find(Filters.eq("name", employee.name)).headL)
          val r2 = companySingle
            .insertOne(company)
            .flatMap(_ => companySource.find(Filters.eq("name", company.name)).headL)
          Task.parZip2(r1, r2)
      }.runSyncUnsafe()

      //then
      r1 shouldBe employee
      r2 shouldBe company
    }

    protected[this] def abstractCreateConnectionTest3(
                                                       makeResource: (
                                                         CollectionRef[Company],
                                                           CollectionRef[Employee],
                                                           CollectionRef[Investor]) => Resource[Task, Tuple3F[CollectionOperator, Company, Employee, Investor]])
    : Assertion = {
      //given
      val employees = List(Employee("Caroline", 21, "Barcelona", "OldCompany"))
      val company = Company("OldCompany", employees, 0)
      val investor1 = Investor("MyInvestor1", 10001, List(company))
      val investor2 = Investor("MyInvestor2", 20001, List(company))
      MongoSingle.insertMany(investorsMongoCol, List(investor1, investor2)).runSyncUnsafe()
      MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()
      MongoSingle.insertOne(companiesMongoCol, company).runSyncUnsafe()
      //and
      val companiesCol = CollectionCodec(
        dbName,
        companiesColName,
        classOf[Company],
        createCodecProvider[Company](),
        createCodecProvider[Employee]())
      val employeesCol =
        CollectionCodec(dbName, employeesColName, classOf[Employee], createCodecProvider[Employee]())
      val investorsCol = CollectionCodec(
        dbName,
        investorsColName,
        classOf[Investor],
        createCodecProvider[Investor](),
        createCodecProvider[Company](),
        createCodecProvider[Employee]())

      val connection = makeResource(companiesCol, employeesCol, investorsCol)

      //when
      val updateResult = connection.use {
        case (
          CollectionOperator(_, companySource, companySingle, companySink),
          CollectionOperator(_, employeeSource, employeeSingle, employeeSink),
          CollectionOperator(_, investorSource, investorSingle, _)) =>
          for {
            _ <- companySingle
              .insertOne(Company("NewCompany", employees = List.empty, investment = 0))
              .delayResult(1.second)
            _ <- {
              employeeSource
                .find(Filters.eq("companyName", "OldCompany")) //read employees from old company
                .bufferTimedAndCounted(2.seconds, 15)
                .map { employees =>
                  // pushes them into the new one
                  (Filters.eq("name", "NewCompany"), Updates.pushEach("employees", employees.asJava))
                }
                .consumeWith(companySink.updateOne())
            }
            //aggregates all the
            investment <- investorSource.find(Filters.in("companies.name", "OldCompany")).map(_.funds).sumL
            updateResult <- companySingle.updateMany(
              Filters.eq("name", "NewCompany"),
              Updates.set("investment", investment))

          } yield updateResult
      }.runSyncUnsafe()

      //then
      updateResult.wasAcknowledged shouldBe true
      updateResult.matchedCount shouldBe 1

      //and
      val newCompany = MongoSource.find(companiesMongoCol, Filters.eq("name", "NewCompany")).headL.runSyncUnsafe()
      newCompany.employees should contain theSameElementsAs employees
      newCompany.investment shouldBe investor1.funds + investor2.funds
    }

  }
}
