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

import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.domain.MongoConnector
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.bson.conversions.Bson
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class MongoSinkSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {

  override def beforeEach() = {
    super.beforeEach()
    MongoDb.dropCollection(db, employeesColName).runSyncUnsafe()
  }

  "deleteOne" should "delete single elements by filters" in {
    //given
    val e1 = Gen.nonEmptyListOf(genEmployee).sample.get
    val e2 = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoSingle.insertMany(employeesMongoCol, e1 ++ e2).runSyncUnsafe()

    //when
    Observable
      .from(e1)
      .map(elem => Filters.eq("name", elem.name))
      .consumeWith(MongoSink.deleteOne(employeesMongoCol))
      .runSyncUnsafe()

    //then
    val r = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    r should contain theSameElementsAs e2
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val e1 = Gen.nonEmptyListOf(genEmployee).sample.get
    val e2 = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoSingle.insertMany(employeesMongoCol, e1 ++ e2).runSyncUnsafe()

    //when
    MongoConnection
      .create1(mongoEndpoint, employeesCol)
      .use {
        case MongoConnector(_, _, _, sink) =>
          Observable
            .from(e1)
            .map(elem => Filters.eq("name", elem.name))
            .consumeWith(sink.deleteOne())
      }.runSyncUnsafe()
    //then
    val r = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    r should contain theSameElementsAs e2
  }

    "deleteMany" should "delete multiple documents per each emitted filter" in {
    //given
    val germans = genEmployeesWith(city = Some("Munich")).sample.get
    val italians = genEmployeesWith(city = Some("Rome")).sample.get
    val turks = genEmployeesWith(city = Some("Istanbul")).sample.get
    val egyptians = genEmployeesWith(city = Some("El Caire")).sample.get
    MongoSingle.insertMany(employeesMongoCol, germans ++ italians ++ turks ++ egyptians).runSyncUnsafe()

    //when
    Observable
      .from(List("Munich", "Rome", "Istanbul"))
      .map(city => Filters.eq("city", city))
      .consumeWith(MongoSink.deleteMany(employeesMongoCol))
      .runSyncUnsafe()

    //then
    val r = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    r should contain theSameElementsAs egyptians
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val germans = genEmployeesWith(city = Some("Munich")).sample.get
    val italians = genEmployeesWith(city = Some("Rome")).sample.get
    val turks = genEmployeesWith(city = Some("Istanbul")).sample.get
    val egyptians = genEmployeesWith(city = Some("El Caire")).sample.get
    MongoSingle.insertMany(employeesMongoCol, germans ++ italians ++ turks ++ egyptians).runSyncUnsafe()

    //when
    MongoConnection
      .create1(mongoEndpoint, employeesCol)
      .use {
        case MongoConnector(_, _, _, sink) =>
          Observable
            .from(List("Munich", "Rome", "Istanbul"))
            .map(city => Filters.eq("city", city))
            .consumeWith(sink.deleteMany())
      }
      .runSyncUnsafe()

    //then
    val r = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    r should contain theSameElementsAs egyptians
  }

  "insertOne" should "insert a single document per each received element" in {
    //given
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get

    //when
    Observable.from(employees).consumeWith(MongoSink.insertOne(employeesMongoCol)).runSyncUnsafe()

    //then
    val r = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    r should contain theSameElementsAs employees
  }

  it should "insert zero documents for empty observables" in {
    //given/when
    Observable.empty.consumeWith(MongoSink.insertOne(employeesMongoCol)).runSyncUnsafe()

    //then
    MongoSource.countAll(employeesMongoCol).runSyncUnsafe() shouldBe 0L
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get

    //when
    MongoConnection
      .create1(mongoEndpoint, employeesCol)
      .use {
        case MongoConnector(_, _, _, sink) =>
          Observable.from(employees).consumeWith(sink.insertOne())
      }
      .runSyncUnsafe()

    //then
    val r = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    r should contain theSameElementsAs employees
  }

  "insertMany" should "insert documents in batches" in {
    //given
    val n = 20
    val employees = Gen.listOfN(n, genEmployee).sample.get

    //when
    Observable
      .from(employees)
      .bufferTumbling(5)
      .consumeWith(MongoSink.insertMany(employeesMongoCol))
      .runSyncUnsafe()

    //then
    val r = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    r should contain theSameElementsAs employees
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val n = 20
    val employees = Gen.listOfN(n, genEmployee).sample.get

    //when
    MongoConnection
      .create1(mongoEndpoint, employeesCol)
      .use {
        case MongoConnector(_, _, _, sink) =>
          Observable
            .from(employees)
            .bufferTumbling(5)
            .consumeWith(sink.insertMany())
      }
      .runSyncUnsafe()
    //then
    val r = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    r should contain theSameElementsAs employees
  }

  "updateOne" should "update a single document per each received element" in {
    //given
    val porto = "Porto"
    val lisbon = "Lisbon"
    val age = 45
    val employees = genEmployeesWith(city = Some(porto), age = Some(age), n = 10).sample.get
    MongoOp.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //and
    val filter = Filters.eq("city", porto)
    val update = Updates.set("city", lisbon)
    val updates: Seq[(Bson, Bson)] = List.fill(4)((filter, update))

    //when
    Observable.from(updates).consumeWith(MongoSink.updateOne(employeesMongoCol)).runSyncUnsafe()

    //then
    val r: Seq[Employee] = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()
    r.size shouldBe employees.size
    r.filter(_.city == porto).size shouldBe employees.size - updates.size
    r.filter(e => (e.city == lisbon)).size shouldBe updates.size
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val porto = "Porto"
    val lisbon = "Lisbon"
    val age = 45
    val employees = genEmployeesWith(city = Some(porto), age = Some(age), n = 10).sample.get
    MongoOp.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //and
    val filter = Filters.eq("city", porto)
    val update = Updates.set("city", lisbon)
    val updates: Seq[(Bson, Bson)] = List.fill(4)((filter, update))

    //when
    MongoConnection
      .create1(mongoEndpoint, employeesCol)
      .use {
        case MongoConnector(_, _, _, sink) =>
          Observable.from(updates).consumeWith(sink.updateOne())
      }
      .runSyncUnsafe()

    //then
    val r: Seq[Employee] = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()
    r.size shouldBe employees.size
    r.filter(_.city == porto).size shouldBe employees.size - updates.size
    r.filter(e => (e.city == lisbon)).size shouldBe updates.size
  }

  "replaceOne" should "replace a single document per each received element" in {
    //given
    val e1 = Employee("Employee1", 45, "Rio")
    val e2 = Employee("Employee2", 34, "Rio")
    MongoOp.insertMany(employeesMongoCol, List(e1, e2)).runSyncUnsafe()

    //and
    val t1 = (Filters.eq("name", "Employee1"), Employee("Employee3", 43, "Rio"))
    val t2 = (Filters.eq("name", "Employee2"), Employee("Employee4", 37, "Rio"))
    val replacements: Seq[(Bson, Employee)] = List(t1, t2)

    //when
    Observable.from(replacements).consumeWith(MongoSink.replaceOne(employeesMongoCol)).runSyncUnsafe()

    //then
    val r: Seq[Employee] = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()
    r.size shouldBe replacements.size
    r should contain theSameElementsAs replacements.map(_._2)
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val e1 = Employee("Employee1", 45, "Rio")
    val e2 = Employee("Employee2", 34, "Rio")
    MongoSingle.insertMany(employeesMongoCol, List(e1, e2)).runSyncUnsafe()

    //and
    val t1 = (Filters.eq("name", "Employee1"), Employee("Employee3", 43, "Rio"))
    val t2 = (Filters.eq("name", "Employee2"), Employee("Employee4", 37, "Rio"))
    val replacements: Seq[(Bson, Employee)] = List(t1, t2)

    //when
    MongoConnection
      .create1(mongoEndpoint, employeesCol)
      .use {
        case MongoConnector(_, _, _, sink) =>
          Observable.from(replacements).consumeWith(sink.replaceOne())
      }
      .runSyncUnsafe()

    //then
    val r: Seq[Employee] = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()
    r.size shouldBe replacements.size
    r should contain theSameElementsAs replacements.map(_._2)
  }

  "updateMany" should "update many documents per each received request" in {
    //given
    val name1 = "Name1"
    val name2 = "Name2"
    val name3 = "Name3"
    val e1 = genEmployeesWith(name = Some(name1), n = 10).sample.get
    val e2 = genEmployeesWith(name = Some(name2), age = Some(31), n = 20).sample.get
    val e3 = genEmployeesWith(name = Some(name3), n = 30).sample.get
    MongoOp.insertMany(employeesMongoCol, e1 ++ e2 ++ e3).runSyncUnsafe()

    //and two update elements
    val u1 = (Filters.eq("name", name1), Updates.set("name", name3))
    val u2 = (Filters.eq("name", name2), Updates.combine(Updates.set("name", name1), Updates.inc("age", 10)))
    val updates: Seq[(Bson, Bson)] = List(u1, u2)

    //when
    Observable.from(updates).consumeWith(MongoSink.updateMany(employeesMongoCol)).runSyncUnsafe()

    //then
    val r: Seq[Employee] = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()
    r.size shouldBe e1.size + e2.size + e3.size
    r.filter(_.name == name3).size shouldBe (e1 ++ e3).size
    r.filter(_.name == name1).size shouldBe e2.map(_.copy(name = name1)).size
    r.filter(_.name == name1) should contain theSameElementsAs e2
      .map(_.copy(name = name1))
      .map(e => e.copy(age = e.age + 10))
  }

  it should "update many documents per each received request (list example)" in {
    //given
    val e = {
      for {
        e1 <- genEmployeesWith(n = 10, activities = List("Table tennis"))
        e2 <- genEmployeesWith(city = Some("Dubai"), n = 4, activities = List("Table tennis"))
      } yield e1 ++ e2
    }.sample.get
    val cities: Set[String] = e.map(_.city).distinct.toSet
    MongoSingle.insertMany(employeesMongoCol, e).runSyncUnsafe()

    //when
    Observable
      .from(cities)
      .map(city => (Filters.eq("city", city), Updates.pull("activities", "Table Tennis")))
      .consumeWith(MongoSink.updateMany(employeesMongoCol))
      .runSyncUnsafe()

    //then
    val r: Seq[Employee] = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()
    r.size shouldBe e.size
    r.filter(_.activities.contains("Table Tennis")) shouldBe empty
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val e = {
      for {
        e1 <- genEmployeesWith(n = 10, activities = List("Table tennis"))
        e2 <- genEmployeesWith(city = Some("Dubai"), n = 4, activities = List("Table tennis"))
      } yield e1 ++ e2
    }.sample.get
    val cities: Set[String] = e.map(_.city).distinct.toSet
    MongoOp.insertMany(employeesMongoCol, e).runSyncUnsafe()

    //when
    MongoConnection
      .create1(mongoEndpoint, employeesCol)
      .use {
        case MongoConnector(_, _, _, sink) =>
          Observable
            .from(cities)
            .map(city => (Filters.eq("city", city), Updates.pull("activities", "Table Tennis")))
            .consumeWith(sink.updateMany())
      }
      .runSyncUnsafe()

    //then
    val r: Seq[Employee] = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()
    r.size shouldBe e.size
    r.filter(_.activities.contains("Table Tennis")) shouldBe empty
  }

}
