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
import monix.connect.mongodb.client.{CollectionOperator, MongoConnection}
import monix.execution.Scheduler
import monix.reactive.Observable
import monix.testing.scalatest.MonixTaskSpec
import org.bson.conversions.Bson
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class MongoSinkSuite extends AsyncFlatSpec with MonixTaskSpec with Fixture with Matchers with BeforeAndAfterEach {

  override implicit val scheduler: Scheduler = Scheduler.io("mongo-sink-suite")

  "deleteOne" should "delete single elements by filters" in {
    val e1 = Gen.nonEmptyListOf(genEmployee).sample.get
    val e2 = Gen.nonEmptyListOf(genEmployee).sample.get
    val employeesColRef = randomEmployeesMongoCol
    MongoSingle.insertMany(employeesColRef, e1 ++ e2) *>
      Observable
        .from(e1)
        .map(elem => Filters.eq("name", elem.name))
        .consumeWith(MongoSink.deleteOne(employeesColRef)) *>
      MongoSource.findAll(employeesColRef)
        .toListL
        .asserting(_ should contain theSameElementsAs e2)
  }

  "deleteOnePar" should "delete single elements by grouped filters, the single group of filters is executed " +
    "at once in parallel" in {
    val e1 = Gen.nonEmptyListOf(genEmployee).sample.get
    val e2 = Gen.nonEmptyListOf(genEmployee).sample.get
    val e3 = Gen.nonEmptyListOf(genEmployee).sample.get

    val employeesColRef = randomEmployeesMongoCol
    MongoSingle.insertMany(employeesColRef, e1 ++ e2 ++ e3) *>
      Observable
        .from(List(e1, e2))
        .map(es => es.map(e => Filters.eq("name", e.name)))
        .consumeWith(MongoSink.deleteOnePar(employeesColRef)) *>
      MongoSource.findAll(employeesColRef)
        .toListL
        .asserting(_ should contain theSameElementsAs e3)
  }

    "deleteMany" should "delete multiple documents per each emitted filter" in {
      val germans = genEmployeesWith(city = Some("Munich")).sample.get
      val italians = genEmployeesWith(city = Some("Rome")).sample.get
      val turks = genEmployeesWith(city = Some("Istanbul")).sample.get
      val egyptians = genEmployeesWith(city = Some("El Caire")).sample.get
      val employeesCol = randomEmployeesMongoCol

      MongoSingle.insertMany(employeesCol, germans ++ italians ++ turks ++ egyptians) *>
        Observable
          .from(List("Munich", "Rome", "Istanbul"))
          .map(city => Filters.eq("city", city))
          .consumeWith(MongoSink.deleteMany(employeesCol)) *>
        MongoSource.findAll(employeesCol)
          .toListL
          .asserting(_ should contain theSameElementsAs egyptians)
  }

  "deleteManyPar" should "delete multiple documents per each emitted grouped filters, the single group " +
    "of filters is executed at once in parallel" in {
    val germans = genEmployeesWith(city = Some("Munich")).sample.get
    val italians = genEmployeesWith(city = Some("Rome")).sample.get
    val turks = genEmployeesWith(city = Some("Istanbul")).sample.get
    val egyptians = genEmployeesWith(city = Some("El Caire")).sample.get
    val employeesCol = randomEmployeesMongoCol

    MongoSingle.insertMany(employeesCol, germans ++ italians ++ turks ++ egyptians) *>
      Observable
        .from(List(List("Munich", "Rome", "Istanbul")))
        .map(cityList => cityList.map(city => Filters.eq("city", city)))
        .consumeWith(MongoSink.deleteManyPar(employeesCol)) *>
      MongoSource.findAll(employeesCol)
        .toListL
        .asserting(_ should contain theSameElementsAs egyptians)
  }

  "insertOne" should "insert a single document per each received element" in {
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      Observable.from(employees).consumeWith(operator.sink.insertOne()) *>
        operator.source.findAll.toListL
    }.asserting(_ should contain theSameElementsAs employees)
  }

  it should "insert zero documents for empty observables" in {
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      Observable.empty.consumeWith(operator.sink.insertOne()) *>
        operator.source.countAll
    }.asserting(_ shouldBe 0L)
  }

  "insertOnePar" should "insert a single document per each received element from a group where " +
    "elements from a single group are inserted at once in parallel" in {
    val employees = Gen.listOfN(15, genEmployee).sample.get.grouped(3).toList
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      Observable.from(employees).consumeWith(operator.sink.insertOnePar()) *>
        operator.source.findAll.toListL
    }.asserting(_ should contain theSameElementsAs employees.flatten)
  }

  "insertMany" should "insert documents in batches" in {
    val n = 20
    val employees = Gen.listOfN(n, genEmployee).sample.get
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      Observable
        .from(employees)
        .bufferTumbling(5)
        .consumeWith(operator.sink.insertMany()) *>
        operator.source.findAll
          .toListL
    }.asserting(_ should contain theSameElementsAs employees)
  }

  "updateOne" should "update a single document per each received element" in {
    val porto = "Porto"
    val lisbon = "Lisbon"
    val age = 45
    val employees = genEmployeesWith(city = Some(porto), age = Some(age), n = 10).sample.get
    val filter = Filters.eq("city", porto)
    val update = Updates.set("city", lisbon)
    val updates: Seq[(Bson, Bson)] = List.fill(4)((filter, update))
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) *>
        Observable.from(updates).consumeWith(operator.sink.updateOne()) *>
        operator.source.findAll
          .toListL
    } .asserting { employees =>
      employees.size shouldBe employees.size
      employees.count(_.city == porto) shouldBe employees.size - updates.size
      employees.count(_.city == lisbon) shouldBe updates.size
    }
  }

  "updateOnePar" should "update a single document per each received element whereas elements are grouped in lists. " +
    "Elements from a single group are executed at once in parallel." in {
    val porto = "Porto"
    val lisbon = "Lisbon"
    val age = 45
    val employees = genEmployeesWith(city = Some(porto), age = Some(age), n = 10).sample.get
    val filter = Filters.eq("city", porto)
    val update = Updates.set("city", lisbon)
    val updates: Seq[(Bson, Bson)] = List.fill(4)((filter, update))

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) *>
        Observable.from(List(updates.take(2), updates.drop(2))).consumeWith(operator.sink.updateOnePar()) *>
        operator.source.findAll
          .toListL
    } .asserting { employees =>
      employees.size shouldBe employees.size
      employees.count(_.city == porto) shouldBe employees.size - updates.size
      employees.count(_.city == lisbon) shouldBe updates.size
    }
  }

  "replaceOne" should "replace a single document per each received element" in {
    val e1 = Employee("Employee1", 45, "Rio")
    val e2 = Employee("Employee2", 34, "Rio")
    val t1 = (Filters.eq("name", "Employee1"), Employee("Employee3", 43, "Rio"))
    val t2 = (Filters.eq("name", "Employee2"), Employee("Employee4", 37, "Rio"))
    val replacements: Seq[(Bson, Employee)] = List(t1, t2)

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(List(e1, e2)) *>
        Observable.from(replacements).consumeWith(operator.sink.replaceOne()) *>
        operator.source.findAll.toListL
    }.asserting { employees =>
      employees.size shouldBe replacements.size
      employees should contain theSameElementsAs replacements.map(_._2)
    }
  }

  "replaceOnePar" should "replace a single document per each received element whereas elements are grouped in lists. " +
    "Elements from a single group are executed at once in parallel." in {
    val e1 = Employee("Employee1", 45, "Rio")
    val e2 = Employee("Employee2", 34, "Rio")
    val t1 = (Filters.eq("name", "Employee1"), Employee("Employee3", 43, "Rio"))
    val t2 = (Filters.eq("name", "Employee2"), Employee("Employee4", 37, "Rio"))
    val replacements: Seq[(Bson, Employee)] = List(t1, t2)

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(List(e1, e2)) *>
        Observable.from(List(replacements)).consumeWith(operator.sink.replaceOnePar()) *>
        operator.source.findAll.toListL
    }.asserting { employees =>
      employees.size shouldBe replacements.size
      employees should contain theSameElementsAs replacements.map(_._2)
    }
  }

  "updateMany" should "update many documents per each received request" in {
    val name1 = "Name1"
    val name2 = "Name2"
    val name3 = "Name3"
    val e1 = genEmployeesWith(name = Some(name1), n = 10).sample.get
    val e2 = genEmployeesWith(name = Some(name2), age = Some(31), n = 20).sample.get
    val e3 = genEmployeesWith(name = Some(name3), n = 30).sample.get

    val u1 = (Filters.eq("name", name1), Updates.set("name", name3))
    val u2 = (Filters.eq("name", name2), Updates.combine(Updates.set("name", name1), Updates.inc("age", 10)))
    val updates: Seq[(Bson, Bson)] = List(u1, u2)

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(e1 ++ e2 ++ e3) *>
        Observable.from(updates).consumeWith(operator.sink.updateMany()) *>
        operator.source.findAll.toListL
    }.asserting { employees =>
      employees.size shouldBe e1.size + e2.size + e3.size
      employees.count(_.name == name3) shouldBe (e1 ++ e3).size
      employees.count(_.name == name1) shouldBe e2.map(_.copy(name = name1)).size
      employees.filter(_.name == name1) should contain theSameElementsAs e2
        .map(_.copy(name = name1))
        .map(e => e.copy(age = e.age + 10))
    }
  }

  it should "update many documents per each received request (list example)" in {
    val e = {
      for {
        e1 <- genEmployeesWith(n = 10, activities = List("Table tennis"))
        e2 <- genEmployeesWith(city = Some("Dubai"), n = 4, activities = List("Table tennis"))
      } yield e1 ++ e2
    }.sample.get
    val cities: Set[String] = e.map(_.city).distinct.toSet

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(e) *>
        Observable
          .from(cities)
          .map(city => (Filters.eq("city", city), Updates.pull("activities", "Table Tennis")))
          .consumeWith(operator.sink.updateMany()) *>
        operator.source.findAll
          .toListL
    }.asserting { employees =>
      employees.size shouldBe e.size
      employees.filter(_.activities.contains("Table Tennis")) shouldBe empty
    }
  }

  "updateManyPar" should "update many documents per each received request whereas requests are grouped in lists. " +
    "Requests from a single group are executed at once in parallel." in {
    val name1 = "Name1"
    val name2 = "Name2"
    val name3 = "Name3"
    val name4 = "Name4"
    val e1 = genEmployeesWith(name = Some(name1), n = 10).sample.get
    val e2 = genEmployeesWith(name = Some(name2), age = Some(31), n = 20).sample.get
    val e3 = genEmployeesWith(name = Some(name3), n = 30).sample.get

    val u1 = (Filters.eq("name", name1), Updates.set("name", name3))
    val u2 = (Filters.eq("name", name2), Updates.combine(Updates.set("name", name4), Updates.inc("age", 10)))
    val updates: Seq[(Bson, Bson)] = List(u1, u2)

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(e1 ++ e2 ++ e3) *>
        Observable.from(List(updates)).consumeWith(operator.sink.updateManyPar()) *>
        operator.source.findAll.toListL
    }.asserting { employees =>
      employees.size shouldBe e1.size + e2.size + e3.size
      employees.count(_.name == name3) shouldBe (e1 ++ e3).size
      employees.count(_.name == name1) shouldBe 0
      employees.count(_.name == name4) shouldBe e2.map(_.copy(name = name4)).size
      employees.filter(_.name == name4) should contain theSameElementsAs e2
        .map(_.copy(name = name4))
        .map(e => e.copy(age = e.age + 10))
    }
  }

}
