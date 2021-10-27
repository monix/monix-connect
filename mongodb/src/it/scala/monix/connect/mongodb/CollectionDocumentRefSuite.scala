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
import monix.connect.mongodb.client.{CollectionCodecRef, CollectionDocumentRef, CollectionOperator, MongoConnection}
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.bson.Document
import org.bson.conversions.Bson
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import org.scalatest.{Assertion, BeforeAndAfterEach}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.concurrent.duration._

class CollectionDocumentRefSuite extends AsyncFlatSpec with MonixTaskSpec with Fixture with Matchers {

  override implicit val scheduler: Scheduler = Scheduler.io("collection-document-ref-suite")

  "A single bson collection" should "be created given the url endpoint" in {
    val collectionName = randomBsonColName
    val name = "Alice"
    val age = 22
    val person = Document.parse(s"""{"name":"$name", "age":$age }""")

    val connection = MongoConnection
      .create1(
        mongoEndpoint,
        CollectionDocumentRef(
          dbName,
          collectionName)
      )

    connection.use {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(person).delayResult(1.second) >> source.find(Filters.eq("name", name)).headL
    }.asserting(_ shouldBe person)
  }

  it should "be created given the mongo client settings" in {
    val bsonCol = randomBsonColRef
    val name = "Margaret"
    val age = 54
    val margaret: Document = Document.parse(s"""{"name":"$name", "age":$age }""")

    val bsonConnection: Resource[Task, CollectionOperator[Bson]] = MongoConnection.create1(
      mongoClientSettings,
      bsonCol)

    bsonConnection.use {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(margaret) >> Task.sleep(1.second) >> source.find(Filters.eq("name", name)).headL
    }.asserting(_ shouldBe margaret)
  }

  it should "be created unsafely given a mongo client" in {
    val bsonCol = randomBsonColRef
    val name = "Greg"
    val age = 41
    val greg: Document = Document.parse(s"""{"name":"$name", "age":$age }""")

    val connection = MongoConnection.createUnsafe1(MongoClients.create(mongoEndpoint), bsonCol)

    connection.flatMap {
      case CollectionOperator(_, source, single, _) =>
        single.insertOne(greg) >> Task.sleep(1.second) >> source.find(Filters.eq("name", name)).headL
    }.asserting(_ shouldBe greg)
  }

  "Two generic document collections" can "be created within the same connection" in {
    val (bsonCol1, bsonCol2) = (randomBsonColRef, randomBsonColRef)
    val personName = "Adrian"
    val filmName = "Jumanji"
    val connection = MongoConnection.create2(mongoEndpoint, (bsonCol1, bsonCol2))
    val person: Document = Document.parse(s"""{"person_name":"$personName", "age": 23 }""")
    val film: Document = Document.parse(s"""{"film_name":"$filmName", "year": 1995}""")

    connection.use[Task, Assertion] {
      case (personOperator, filmOperator) =>
        for {
          r1 <- personOperator.single.insertOne(person).delayResult(1.second) >>
            personOperator.source.find(Filters.eq("person_name", personName)).headL
          r2 <- filmOperator.single.insertOne(film) >>
            filmOperator.source.find(Filters.eq("film_name", filmName)).headL
        } yield {
          r1 shouldBe person
          r2 shouldBe film
        }
    }
  }

  "Two mixed collections" can "be created within the same connection" in {
    val filmName = "Jumanji"
    val film: Document = Document.parse(s"""{"film_name":"$filmName", "year": 1995}""")
    val employee1 = Employee("Employee1", 21, "Paris", "Company1")
    val employee2 = Employee("Employee2", 29, "Amsterdam", "Company2")
    val filmsCol = CollectionDocumentRef(dbName, "films_collection")

    val connection = MongoConnection.create2(mongoEndpoint, (randomEmployeesColRef, filmsCol))

    connection.use[Task, Assertion] { case (employeesOperator, filmsOperator) =>
        for {
          _ <- employeesOperator.single.insertMany(List(employee1, employee2)) >> filmsOperator.single.insertOne(film).delayResult(1.second)
          parisEmployeesCount <- employeesOperator.source.count(Filters.eq("city", "Paris"))
          myOnlyFilm <- filmsOperator.source.find(Filters.eq("film_name", "Jumanji")).headL
        } yield {
          parisEmployeesCount shouldBe 1L
          myOnlyFilm shouldBe film
        }
    }
  }

  "Three mixed collections" should "be created within the same connection" in {
    val (bsonCol1, bsonCol2) = (randomBsonColRef, randomBsonColRef)

    val personName = "Jim Carrey"
    val filmName = "The Mask"
    val person: Document = Document.parse(s"""{"person_name":"$personName", "age": 23 }""")
    val film: Document = Document.parse(s"""{"film_name":"$filmName", "year": 1995}""")
    val employee1 = Employee("Caroline", 21, "Barcelona", "Company1")
    val employee2 = Employee("Joana", 29, "Amsterdam", "Company2")


    val connection = MongoConnection.create3(mongoEndpoint, (bsonCol1, bsonCol2, randomEmployeesColRef))

    connection.use[Task, Assertion] {
      case (
        bson1Operator,
        bson2Operator,
        employeesOperator) =>
        for {
          employee <- employeesOperator.single.insertMany(List(employee1, employee2)).delayResult(1.second) >>
            employeesOperator.source.find(Filters.eq("companyName", "Company1")).toListL
          foundPerson <- bson1Operator.single.insertOne(person) >> bson1Operator.source.find(Filters.eq("person_name", personName)).headL
          foundFilm <- bson2Operator.single.insertOne(film).delayResult(1.second) >>
            bson2Operator.source.find(Filters.eq("film_name", filmName)).headL
        } yield {
          employee shouldBe List(employee1)
          foundPerson shouldBe person
          foundFilm shouldBe film
        }
    }

  }

}
