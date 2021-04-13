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

import com.mongodb.client.model.{Accumulators, Aggregates, CountOptions, Filters, Updates}
import monix.connect.mongodb.client.{CollectionOperator, MongoConnection}
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
    MongoDb.dropCollection(db, employeesColName).runSyncUnsafe()
    MongoDb.dropCollection(db, companiesColName).runSyncUnsafe()
  }

  s"aggregate" should  "aggregate with a match aggregation" in {
    //given
    val oldEmployees = genEmployeesWith(age = Some(55)).sample.get
    val youngEmployees = genEmployeesWith(age = Some(22)).sample.get
    MongoSingle.insertMany(employeesMongoCol, youngEmployees ++ oldEmployees).runSyncUnsafe()

    //when
    val aggregation =  Aggregates.`match`(Filters.gt("age", 35))
    val aggregated: Seq[Employee] = MongoSource.aggregate[Employee, Employee](employeesMongoCol, Seq(aggregation), classOf[Employee]).toListL.runSyncUnsafe()

    //then
    aggregated.size shouldBe oldEmployees.size
  }

  it should "aggregate with group by" in {
    //given
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val aggregation =  Aggregates.group("group", Accumulators.avg("average", "$age"))
    val aggregated: List[Document] = MongoSource.aggregate[Employee](employeesMongoCol, Seq(aggregation)).toListL.runSyncUnsafe()

    //then
    aggregated.head.getDouble("average") shouldBe (employees.map(_.age).sum.toDouble / employees.size)
  }

  it should "pipes multiple aggregations" in {
    //given
    val e1 = genEmployeeWith(age = Some(55)).sample.get
    val e2 = genEmployeeWith(age = Some(65)).sample.get
    val e3 = genEmployeeWith(age = Some(22)).sample.get
    MongoSingle.insertMany(employeesMongoCol, List(e1, e2, e3)).runSyncUnsafe()

    //when
    val matchAgg = Aggregates.`match`(Filters.gt("age", 35))
    val groupAgg = Aggregates.group("group", Accumulators.avg("average", "$age"))
    val aggregated = MongoSource.aggregate[Employee](employeesMongoCol, Seq(matchAgg, groupAgg)).toListL.runSyncUnsafe()

    //then
    aggregated.head.getDouble("average") shouldBe 60
  }

  it should "aggregate by unwind" in {
    val hobbies = List("reading", "running", "programming")
    val employee: Employee = genEmployeeWith(city = Some("Toronto"), activities = hobbies).sample.get
    MongoSingle.insertOne(employeesMongoCol, employee).runSyncUnsafe()

    val filter = Aggregates.`match`(Filters.eq("city", "Toronto"))
    val unwind = Aggregates.unwind("$activities")

    val unwinded: Seq[UnwoundEmployee] = MongoSource.aggregate[Employee, UnwoundEmployee](employeesMongoCol, Seq(filter, unwind), classOf[UnwoundEmployee])
      .toListL
      .runSyncUnsafe()

    unwinded.size shouldBe 3
    unwinded.map(_.activities) should contain theSameElementsAs hobbies
  }

  "count" should  "count all" in {
    //given
    val n = 10
    val employees = Gen.listOfN(n, genEmployee).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val collectionSize = MongoSource.countAll(employeesMongoCol).runSyncUnsafe()

    //then
    collectionSize shouldBe n
  }

  it should  "count by filter" in {
    //given
    val n = 6
    val scottishEmployees = genEmployeesWith(city = Some("Edinburgh"), n = n).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.++(scottishEmployees)).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val filer: Bson = Filters.eq("city", "Edinburgh")
    val nElements = MongoSource.count(employeesMongoCol, filer).runSyncUnsafe()

    //then
    nElements shouldBe scottishEmployees.size
  }


  it should  "count with countOptions" in {
    //given
    val senegalEmployees = genEmployeesWith(city = Some("Dakar")).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.++(senegalEmployees)).sample.get
    val countOptions = new CountOptions().limit(senegalEmployees.size -1)
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val filer: Bson = Filters.eq("city", "Dakar")
    val nElements = MongoSource.count(employeesMongoCol, filer, countOptions).runSyncUnsafe()

    //then
    nElements shouldBe senegalEmployees.size - 1
  }

  it should  "count 0 documents when on empty collections" in {
    //given/when
    val collectionSize = MongoSource.countAll(employeesMongoCol).runSyncUnsafe()

    //then
    collectionSize shouldBe 0L
  }

  "distinct" should  "distinguish unique fields" in {
    //given
    val chineseEmployees = genEmployeesWith(city = Some("Shanghai")).sample.get
    val employees = Gen.nonEmptyListOf(genEmployee).map(_ ++ chineseEmployees).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val distinct: List[String] = MongoSource.distinct(employeesMongoCol, "city", classOf[String]).toListL.runSyncUnsafe()

    //then
    assert(distinct.size < employees.size)
    distinct.filter(_.equals("Shanghai")).size shouldBe 1
  }

  it should "findAll elements in a collection" in {
    //given
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val l = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()

    //then
    l should contain theSameElementsAs employees
  }

  it should  "find no elements" in {
    //given/when
    val l = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()

    //then
    l shouldBe List.empty
  }

  it should  "find filtered elements" in {
    //given
    val miamiEmployees = genEmployeesWith(city = Some("San Francisco")).sample.get
    val employees = Gen.nonEmptyListOf(genEmployee).map(_ ++ miamiEmployees).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val l = MongoSource.find[Employee](employeesMongoCol, Filters.eq("city", "San Francisco")).toListL.runSyncUnsafe()

    //then
    l should contain theSameElementsAs miamiEmployees
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val employee = genEmployeeWith(name = Some("Pablo")).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.:+(employee)).sample.get
    val company = Company("myCompany", employees, 1000)

    //when
    val exists = MongoConnection.create1(mongoEndpoint, companiesCol).use{ case CollectionOperator(_, source, single, _) =>
      for{
        _ <- single.insertOne(company)
        //two different ways to filter the same thing
        exists1 <- source.find(Filters.in("employees", employee)).nonEmptyL
        exists2 <- source.findAll().filter(_.name == company.name).map(_.employees.contains(employee)).headOrElseL(false)
      } yield (exists1 && exists2)
    }.runSyncUnsafe()

    //then
    exists shouldBe true
  }

  "findOneAndDelete" should  "find one document by a given filter, delete it and return it" in {
    //given
    val n = 10
    val employees = genEmployeesWith(n = n, city = Some("Cracow")).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val filter = Filters.eq("city", "Cracow")
    val f = MongoSource.findOneAndDelete[Employee](employeesMongoCol, filter).runSyncUnsafe()

    //then
    f.isDefined shouldBe true
    f.get.city shouldBe "Cracow"

    //and
    val l = MongoSource.findAll[Employee](employeesMongoCol).toListL.runSyncUnsafe()
    l.size shouldBe n - 1
  }

  it should  "not find nor delete when filter didn't find matches" in {
    //given/when
    val filter = Filters.eq("city", "Cairo")
    val f = MongoSource.findOneAndDelete[Employee](employeesMongoCol, filter).runSyncUnsafe()

    //then
    f.isDefined shouldBe false
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val n = 10
    val employees = genEmployeesWith(n = n, city = Some("Zurich")).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees).runSyncUnsafe()

    //when
    val filter = Filters.eq("city", "Zurich")
    val r = MongoConnection.create1(mongoEndpoint, employeesCol).use(_.source.findOneAndDelete(filter)).runSyncUnsafe()

    //then
    r.isDefined shouldBe true
    r.get.city shouldBe "Zurich"

    //and
    val l = MongoSource.findAll(employeesMongoCol).toListL.runSyncUnsafe()
    l.size shouldBe n - 1
  }

    "replaceOne" should "find and replace one single document" in {
    //given
    val employeeA = genEmployeeWith(name = Some("Beth")).sample.get
    val employeeB = genEmployeeWith(name = Some("Samantha")).sample.get

    //and
    MongoSingle.insertOne(employeesMongoCol, employeeA).runSyncUnsafe()

    //when
    val r = MongoSource.findOneAndReplace(employeesMongoCol, Filters.eq("name", employeeA.name), employeeB).runSyncUnsafe()

    //then
    r.isDefined shouldBe true
    r.get shouldBe employeeA

    //and
    val replacement = MongoSource.find(employeesMongoCol, Filters.eq("name", employeeB.name)).headL.runSyncUnsafe()
    replacement shouldBe employeeB
  }

  it should "not find nor replace when filter didn't find matches" in {
    //given
    val employee = genEmployeeWith(name = Some("Alice")).sample.get
    val filter: Bson = Filters.eq("name", "Whatever") //it does not exist

    //when
    val r = MongoSource.findOneAndReplace(employeesMongoCol, filter, employee).runSyncUnsafe()

    //then
    r.isDefined shouldBe false

    //and
    val count = MongoSource.count(employeesMongoCol, Filters.eq("name", "Alice")).runSyncUnsafe()
    count shouldBe 0L
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val employeeA = genEmployeeWith(name = Some("Beth")).sample.get
    val employeeB = genEmployeeWith(name = Some("Samantha")).sample.get

    //and
    MongoSingle.insertOne(employeesMongoCol, employeeA).runSyncUnsafe()

    //when
    val r = MongoConnection.create1(mongoEndpoint, employeesCol)
      .use(_.source.findOneAndReplace(Filters.eq("name", employeeA.name), employeeB)).runSyncUnsafe()

    //then
    r.isDefined shouldBe true
    r.get shouldBe employeeA

    //and
    val replacement = MongoSource.find(employeesMongoCol, Filters.eq("name", employeeB.name)).headL.runSyncUnsafe()
    replacement shouldBe employeeB
  }

    "findOneAndUpdate" should "find a single document by a filter, update and return it" in {
    //given
    val employee = genEmployeeWith(name = Some("Glen")).sample.get
    val filter = Filters.eq("name", employee.name)

    //and
    MongoSingle.insertOne(employeesMongoCol, employee).runSyncUnsafe()

    //when
    val update = Updates.inc("age", 1)
    val r = MongoSource.findOneAndUpdate(employeesMongoCol,filter, update).runSyncUnsafe()

    //then
    r.isDefined shouldBe true
    r.get shouldBe employee

    //and
    val updated = MongoSource.find(employeesMongoCol, filter).headL.runSyncUnsafe()
    updated.age shouldBe employee.age + 1
  }

  it should "not find nor update when filter didn't find matches" in {
    //given
    val filter: Bson = Filters.eq("name", "Isabelle") //it does not exist
    val update = Updates.inc("age", 1)

    //when
    val r = MongoSource.findOneAndUpdate(employeesMongoCol, filter, update).runSyncUnsafe()

    //then
    r.isDefined shouldBe false
  }

  it should "be likewise available from within the resource usage" in {
    //given
    val employee = genEmployeeWith(name = Some("Jack")).sample.get
    val filter = Filters.eq("name", employee.name)

    //and
    MongoSingle.insertOne(employeesMongoCol, employee).runSyncUnsafe()

    //when
    val update = Updates.inc("age", 1)
    val r = MongoConnection.create1(mongoEndpoint, employeesCol)
      .use(_.source.findOneAndUpdate(filter, update))
      .runSyncUnsafe()

    //then
    r.isDefined shouldBe true
    r.get shouldBe employee

    //and
    val updated = MongoSource.find(employeesMongoCol, filter).headL.runSyncUnsafe()
    updated.age shouldBe employee.age + 1
  }

  }
