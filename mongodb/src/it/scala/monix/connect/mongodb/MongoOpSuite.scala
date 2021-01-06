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

import org.scalatest.flatspec.AnyFlatSpecLike
import monix.execution.Scheduler.Implicits.global
import com.mongodb.client.model.{Collation, CollationCaseFirst, DeleteOptions, Filters, Updates}
import monix.connect.mongodb.domain.Collection
import org.bson.conversions.Bson
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class MongoOpSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    MongoDb.dropDatabase(db).runSyncUnsafe()
    MongoDb.dropCollection(db, employeesColName).runSyncUnsafe()
  }

  "deleteOne" should "delete one single element by filtering" in {
    //given
    val exampleName = "deleteOneExample"
    val employees = genEmployeesWith(name = Some(exampleName)).sample.get
    MongoOp.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val r = MongoOp.deleteOne(employeesMongoCol, docNameFilter(exampleName)).runSyncUnsafe()

    //then
    val finalElements = MongoSource.count(employeesMongoCol, docNameFilter(exampleName)).runSyncUnsafe()
    r.deleteCount shouldBe 1L
    r.wasAcknowledged shouldBe true
    finalElements shouldBe employees.size - 1
  }

  it should "delete one single element when it does not exists" in {
    //given
    val filter = Filters.eq("name", "deleteWhenNoExists")

    //when
    val r = MongoOp.deleteOne(employeesMongoCol, filter).runSyncUnsafe()

    //then
    val finalElements = MongoSource.countAll(employeesMongoCol).runSyncUnsafe()
    r.deleteCount shouldBe 0L
    r.wasAcknowledged shouldBe true
    finalElements shouldBe 0
  }

  it should "delete one single element with delete options" in {
    //given
    val uppercaseNat = "Japanese"
    val lowercaseNat = "japanese"
    val collation = Collation.builder().collationCaseFirst(CollationCaseFirst.UPPER).locale("es").build()
    val deleteOptions = new DeleteOptions().collation(collation)
    val employee = genEmployeeWith(city = Some(uppercaseNat)).sample.get
    val employees = genEmployeesWith(city = Some(lowercaseNat)).sample.get
    MongoOp.insertOne(employeesMongoCol, employee).runSyncUnsafe()
    MongoOp.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val r = MongoOp.deleteOne(employeesMongoCol, Filters.in("city", lowercaseNat, uppercaseNat), deleteOptions).runSyncUnsafe()

    //then
    val nUppercaseNat = MongoSource.count(employeesMongoCol, Filters.eq("city", uppercaseNat)).runSyncUnsafe()
    r.deleteCount shouldBe 1L
    r.wasAcknowledged shouldBe true
    nUppercaseNat shouldBe 0L
  }

  "deleteMany" should "delete many elements by filter" in { //todo
    //given
    val exampleName = "deleteManyExample"
    val employees = genEmployeesWith(name = Some(exampleName)).sample.get
    MongoOp.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val r = MongoOp.deleteMany(employeesMongoCol, Filters.eq("name", exampleName)).runSyncUnsafe()

    //then
    val finalElements = MongoSource.count(employeesMongoCol, docNameFilter(exampleName)).runSyncUnsafe()
    r.deleteCount shouldBe employees.length
    r.wasAcknowledged shouldBe true
    finalElements shouldBe 0L
  }

    "insertOne"  should "insert one single element" in {
    //given
    val e = genEmployee.sample.get

    //when
    val r = MongoOp.insertOne(employeesMongoCol, e).runSyncUnsafe()

    //then
    r.insertedId.isDefined shouldBe true
    r.wasAcknowledged shouldBe true

    //and
    MongoSource.findAll(employeesMongoCol).headL.runSyncUnsafe() shouldBe e
  }

  "insertMany" should "insert many elements" in {
    //given
    val nationality = "Vancouver"
    val employees = genEmployeesWith(city = Some(nationality)).sample.get

    //when
    val r = MongoOp.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //then
    r.insertedIds.size shouldBe employees.size
    r.wasAcknowledged shouldBe true

    //and
    val elements = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    elements shouldBe employees
  }

  "replaceOne" should "replace one single element" in {
    //given
    val employeeName = "Humberto"
    val e = genEmployeeWith(name = Option(employeeName)).sample.get
    val filter: Bson = Filters.eq("name", employeeName)

    //and
    MongoOp.insertOne(employeesMongoCol, e).runSyncUnsafe()

    //when
    val r = MongoOp.replaceOne(employeesMongoCol, filter, e.copy(age = e.age + 1)).runSyncUnsafe()

    //then
    r.modifiedCount shouldBe 1L

    //and
    val updated = MongoSource.find(employeesMongoCol, filter).headL.runSyncUnsafe()
    updated.age shouldBe e.age + 1
  }

  "updateOne" should "update one single element" in {
    //given
    val cambridge = "Cambridge"
    val oxford = "Oxford"
    val employees = genEmployeesWith(city = Some(cambridge)).sample.get
    MongoOp.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val updateResult = MongoOp.updateOne(employeesMongoCol, nationalityDocument(cambridge), Updates.set("city", oxford)).runSyncUnsafe()

    //then
    val cambridgeEmployeesCount = MongoSource.count(employeesMongoCol, nationalityDocument(cambridge)).runSyncUnsafe()
    val oxfordEmployeesCount = MongoSource.count(employeesMongoCol, nationalityDocument(oxford)).runSyncUnsafe()
    cambridgeEmployeesCount shouldBe employees.size - 1
    oxfordEmployeesCount shouldBe 1
    updateResult.matchedCount shouldBe 1
    updateResult.modifiedCount shouldBe 1
    updateResult.wasAcknowledged shouldBe true
  }

  it should  "update one single element list" in {
    //given
    val employee = genEmployeeWith(city = Some("Galway"), activities = List("Cricket")).sample.get
    MongoOp.insertOne(employeesMongoCol, employee).runSyncUnsafe()

    //when
    val filter = Filters.eq("city", "Galway")
    val update = Updates.push("activities", "Ping Pong")
    val updateResult = MongoOp.updateOne(employeesMongoCol, filter, update).runSyncUnsafe()

    //then
    val r = MongoSource.find(employeesMongoCol, filter).headL.runSyncUnsafe()
    r.activities.contains("Ping Pong") shouldBe true
    updateResult.matchedCount shouldBe 1
    updateResult.modifiedCount shouldBe 1
    updateResult.wasAcknowledged shouldBe true
  }

  "updateMany" should "update many elements" in {
    //given
    val bogota = "Bogota"
    val rio = "Rio"
    val employees = genEmployeesWith(city = Some(bogota)).sample.get
    MongoOp.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val updateResult = MongoOp.updateMany(employeesMongoCol, nationalityDocument(bogota), Updates.set("city", rio)).runSyncUnsafe()

    //then
    updateResult.matchedCount shouldBe employees.size
    updateResult.modifiedCount shouldBe employees.size
    updateResult.wasAcknowledged shouldBe true

    //and
    val colombians = MongoSource.count(employeesMongoCol, nationalityDocument(bogota)).runSyncUnsafe()
    val brazilians = MongoSource.count(employeesMongoCol, nationalityDocument(rio)).runSyncUnsafe()
    colombians shouldBe 0
    brazilians shouldBe employees.size
  }

}
