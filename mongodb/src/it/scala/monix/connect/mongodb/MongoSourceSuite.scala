/*
 * Copyright (c) 2020-2020 by The Monix Connect Project Developers.
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

import com.mongodb.client.model.{Accumulators, Aggregates, CountOptions, Filters}
import monix.execution.Scheduler.Implicits.global
import org.bson.Document
import org.bson.conversions.Bson
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class MongoSourceSuite extends AnyFlatSpecLike with Fixture with Matchers with BeforeAndAfterEach {

  override def beforeEach() = {
    super.beforeEach()
    MongoDb.dropCollection(db, collectionName).runSyncUnsafe()
  }

  s"${MongoSource}" should  "aggregate with a match aggregation" in {
    val oldEmployees = genEmployeesWith(age = Some(55)).sample.get
    val youngEmployees = genEmployeesWith(age = Some(22)).sample.get
    MongoOp.insertMany(col, youngEmployees ++ oldEmployees).runSyncUnsafe()

    val aggregation =  Aggregates.`match`(Filters.gt("age", 35))

    val aggregated: Seq[Employee] = MongoSource.aggregate[Employee, Employee](col, Seq(aggregation), classOf[Employee]).toListL.runSyncUnsafe()

    aggregated.size shouldBe oldEmployees.size
  }

  it should "aggregate with group by" in {
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoOp.insertMany(col, employees).runSyncUnsafe()

    val aggregation =  Aggregates.group("group", Accumulators.avg("average", "$age"))

    val aggregated: List[Document] = MongoSource.aggregate[Employee](col, Seq(aggregation)).toListL.runSyncUnsafe()

    aggregated.head.getDouble("average") shouldBe (employees.map(_.age).sum.toDouble / employees.size)
  }

  it should "pipes multiple aggregations" in {
    val e1 = genEmployeeWith(age = Some(55)).sample.get
    val e2 = genEmployeeWith(age = Some(65)).sample.get
    val e3 = genEmployeeWith(age = Some(22)).sample.get

    MongoOp.insertMany(col, List(e1, e2, e3)).runSyncUnsafe()

    val matchAgg = Aggregates.`match`(Filters.gt("age", 35))
    val groupAgg = Aggregates.group("group", Accumulators.avg("average", "$age"))

    val aggregated = MongoSource.aggregate[Employee](col, Seq(matchAgg, groupAgg)).toListL.runSyncUnsafe()

    aggregated.head.getDouble("average") shouldBe 60
  }

  it should "aggregate by unwind" in {
    val hobbies = List("reading", "running", "programming")
    val employee: Employee = genEmployeeWith(city = Some("Toronto"), activities = hobbies).sample.get
    MongoOp.insertOne(col, employee).runSyncUnsafe()

    val filter = Aggregates.`match`(Filters.eq("city", "Toronto"))
    val unwind = Aggregates.unwind("$activities")

    val unwinded: Seq[UnwoundEmployee] = MongoSource.aggregate[Employee, UnwoundEmployee](col, Seq(filter, unwind), classOf[UnwoundEmployee])
      .toListL
      .runSyncUnsafe()

    unwinded.size shouldBe 3
    unwinded.map(_.activities) should contain theSameElementsAs hobbies
  }

  it should  "count all" in {
    //given
    val n = 10
    val employees = Gen.listOfN(n, genEmployee).sample.get
    MongoOp.insertMany(col, employees).runSyncUnsafe()

    //when
    val collectionSize = MongoSource.countAll(col).runSyncUnsafe()

    //then
    collectionSize shouldBe n
  }

  it should  "count by filter" in {
    //given
    val n = 6
    val scottishEmployees = genEmployeesWith(city = Some("Edinburgh"), n = n).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.++(scottishEmployees)).sample.get
    MongoOp.insertMany(col, employees).runSyncUnsafe()

    //when
    val filer: Bson = Filters.eq("city", "Edinburgh")
    val nElements = MongoSource.count(col, filer).runSyncUnsafe()

    //then
    nElements shouldBe scottishEmployees.size
  }

  it should  "count with countOptions" in {
    //given
    val nationality = "Da"
    val senegalEmployees = genEmployeesWith(city = Some("Dakar")).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.++(senegalEmployees)).sample.get
    val countOptions = new CountOptions().limit(senegalEmployees.size -1)
    MongoOp.insertMany(col, employees).runSyncUnsafe()

    //when
    val filer: Bson = Filters.eq("city", "Dakar")
    val nElements = MongoSource.count(col, filer, countOptions).runSyncUnsafe()

    //then
    nElements shouldBe senegalEmployees.size - 1
  }

  it should  "distinct" in {
    //given
    val chineseEmployees = genEmployeesWith(city = Some("Shanghai")).sample.get
    val employees = Gen.nonEmptyListOf(genEmployee).map(_ ++ chineseEmployees).sample.get
    MongoOp.insertMany(col, employees).runSyncUnsafe()

    //when
    val distinct: List[String] = MongoSource.distinct(col, "city", classOf[String]).toListL.runSyncUnsafe()

    //then
    assert(distinct.size < employees.size)
    distinct.filter(_.equals("Shanghai")).size shouldBe 1
  }

  it should "findAll elements in a collection" in {
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoOp.insertMany(col, employees).runSyncUnsafe()

    //when
    val l: Seq[Employee] = MongoSource.findAll[Employee](col).toListL.runSyncUnsafe()

    //then
    l should contain theSameElementsAs employees
  }

  it should  "find all filtered elements" in {
    val miamiEmployees = genEmployeesWith(city = Some("San Francisco")).sample.get
    val employees = Gen.nonEmptyListOf(genEmployee).map(_ ++ miamiEmployees).sample.get
    MongoOp.insertMany(col, employees).runSyncUnsafe()

    //when
    val l: Seq[Employee] = MongoSource.find[Employee](col, Filters.eq("city", "San Francisco")).toListL.runSyncUnsafe()

    //then
    l should contain theSameElementsAs miamiEmployees
  }

}
