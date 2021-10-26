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

import org.scalatest.flatspec.AsyncFlatSpec
import com.mongodb.client.model.{Collation, CollationCaseFirst, DeleteOptions, Filters, Updates}
import monix.connect.mongodb.client.MongoConnection
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.bson.conversions.Bson
import org.scalatest.{Assertion, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers

class MongoSingleSuite extends AsyncFlatSpec with MonixTaskSpec with Fixture with Matchers with BeforeAndAfterEach {

  override implicit val scheduler: Scheduler = Scheduler.io("mongo-single-suite")

  "deleteOne" should "delete one single document when it does not exists" in {
    val filter = Filters.eq("name", "deleteWhenNoExists")

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        deleteResult <- operator.single.deleteOne(filter)
        finalElements <- operator.source.countAll()
      } yield {
        deleteResult.deleteCount shouldBe 0L
        deleteResult.wasAcknowledged shouldBe true
        finalElements shouldBe 0
      }
    }
  }

  it should "delete one single document with delete options" in {
    val uppercaseNat = "Japanese"
    val lowercaseNat = "japanese"
    val collation = Collation.builder().collationCaseFirst(CollationCaseFirst.UPPER).locale("es").build()
    val deleteOptions = new DeleteOptions().collation(collation)
    val employee = genEmployeeWith(city = Some(uppercaseNat)).sample.get
    val employees = genEmployeesWith(city = Some(lowercaseNat)).sample.get

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertOne(employee)
        _ <- operator.single.insertMany(employees)
        deleteResult <- operator.single.deleteOne(Filters.in("city", lowercaseNat, uppercaseNat), deleteOptions)
        nUppercaseNationality <- operator.source.count(Filters.eq("city", uppercaseNat))
      } yield {
        deleteResult.deleteCount shouldBe 1L
        deleteResult.wasAcknowledged shouldBe true
        nUppercaseNationality shouldBe 0L
      }
    }
  }

  "deleteMany" should "delete many documents by filter" in {
    val exampleName = "deleteManyExample"
    val employees = genEmployeesWith(name = Some(exampleName)).sample.get
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertMany(employees)
        deleteResult <- operator.single.deleteMany(Filters.eq("name", exampleName))
        finalElements <- operator.source.count(docNameFilter(exampleName))
      } yield {
        deleteResult.deleteCount shouldBe employees.length
        deleteResult.wasAcknowledged shouldBe true
        finalElements shouldBe 0L
      }
    }
  }

  it should "delete 0 documents when delete filter didn't find matches" in {
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        initialElements <- operator.source.countAll()
        deleteResult <- operator.single.deleteMany(Filters.eq("name", "exampleName"))
        finalElements <- operator.source.countAll()
      } yield {
        deleteResult.deleteCount shouldBe 0
        deleteResult.wasAcknowledged shouldBe true
        finalElements shouldBe initialElements
      }
    }
  }

  "insertOne" should "insert one single document" in {
    val e = genEmployee.sample.get
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        insertResult <- operator.single.insertOne(e)
        foundDoc <- operator.source.findAll.headL
      } yield {
        insertResult.insertedId.isDefined shouldBe true
        insertResult.wasAcknowledged shouldBe true
        foundDoc shouldBe e
      }
    }
  }

  "insertMany" should "insert many documents" in {
    val nationality = "Vancouver"
    val employees = genEmployeesWith(city = Some(nationality)).sample.get
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        insertResult <- operator.single.insertMany(employees)
        documents <- operator.source.findAll.toListL
      } yield {
        insertResult.insertedIds.size shouldBe employees.size
        insertResult.wasAcknowledged shouldBe true
        documents shouldBe employees
      }
    }
  }

  "replaceOne" should "replace one single document" in {
    val employeeName = "Humberto"
    val e = genEmployeeWith(name = Option(employeeName)).sample.get
    val filter: Bson = Filters.eq("name", employeeName)
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertOne(e)
        replaceResult <- operator.single.replaceOne(filter, e.copy(age = e.age + 1))
        updatedDoc <- operator.source.find(filter).headL
      } yield {
        replaceResult.modifiedCount shouldBe 1L
        updatedDoc.age shouldBe e.age + 1
      }
    }
  }

  it should "not replace any document when filter didn't find matches" in {
    val employeeName = "John"
    val employee = genEmployeeWith(name = Option(employeeName)).sample.get
    val filter: Bson = Filters.eq("name", employeeName) //it does not exist

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      operator.single.replaceOne(filter, employee.copy(age = employee.age + 1))
        .asserting { replaceResult =>
          replaceResult.matchedCount shouldBe 0L
          replaceResult.modifiedCount shouldBe 0L
          replaceResult.wasAcknowledged shouldBe true
        }
    }
  }

  "updateOne" should "update one single document" in {
    val cambridge = "Cambridge"
    val oxford = "Oxford"
    val employees = genEmployeesWith(city = Some(cambridge)).sample.get

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertMany(employees)
        updateResult <-
          operator.single.updateOne(nationalityDocument(cambridge), Updates.set("city", oxford))
        cambridgeEmployeesCount <- operator.source.count(nationalityDocument(cambridge))
        oxfordEmployeesCount <- operator.source.count(nationalityDocument(oxford))
      } yield {
        cambridgeEmployeesCount shouldBe employees.size - 1
        oxfordEmployeesCount shouldBe 1
        updateResult.matchedCount shouldBe 1
        updateResult.modifiedCount shouldBe 1
        updateResult.wasAcknowledged shouldBe true
      }
    }
  }

  it should "update (push) to the list of a single document" in {
    val employee = genEmployeeWith(city = Some("Galway"), activities = List("Cricket")).sample.get
    val filter = Filters.eq("city", "Galway")
    val update = Updates.push("activities", "Ping Pong")

    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertOne(employee)
        updateResult <- operator.single.updateOne(filter, update)
        updatedDoc <- operator.source.find(filter).headL
      } yield {
        updateResult.matchedCount shouldBe 1
        updateResult.modifiedCount shouldBe 1
        updateResult.wasAcknowledged shouldBe true
        updatedDoc.activities.contains("Ping Pong") shouldBe true
      }
    }
  }

  it should "no update document when filter didn't find matches" in {
    val employeeName = "Sebastian"
    val filter: Bson = Filters.eq("name", employeeName)
    val update = Updates.push("activities", "Ping Pong")
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use {
      _.single.updateOne(filter, update)
    }.asserting { updateResult =>
      updateResult.matchedCount shouldBe 0L
      updateResult.modifiedCount shouldBe 0L
      updateResult.wasAcknowledged shouldBe true
    }
  }

  "updateMany" should "update many elements" in {
    val bogota = "Bogota"
    val rio = "Rio"
    val employees = genEmployeesWith(city = Some(bogota)).sample.get
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertMany(employees)
        updateResult <- operator.single.updateMany(nationalityDocument(bogota), Updates.set("city", rio))
        colombians <-  operator.source.count(nationalityDocument(bogota))
        brazilians <-  operator.source.count(nationalityDocument(rio))
      } yield {
        updateResult.matchedCount shouldBe employees.size
        updateResult.modifiedCount shouldBe employees.size
        updateResult.wasAcknowledged shouldBe true
        colombians shouldBe 0
        brazilians shouldBe employees.size
      }
    }
  }

  it should "updateMany returns zero modified count when matched count was also zero" in {
    val employeeName = "Bartolo"

    val filter: Bson = Filters.eq("name", employeeName)
    val update = Updates.push("activities", "Ping Pong")
    MongoConnection
      .create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.updateMany(filter, update)
    }.asserting { r =>
      r.matchedCount shouldBe 0L
      r.modifiedCount shouldBe 0L
      r.wasAcknowledged shouldBe true
    }
  }

}