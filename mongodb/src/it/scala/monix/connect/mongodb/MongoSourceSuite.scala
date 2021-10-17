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
import monix.connect.mongodb.client.{CollectionCodecRef, CollectionOperator, MongoConnection}
import monix.eval.Task
import monix.execution.Scheduler
import monix.testing.scalatest.MonixTaskSpec
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.scalacheck.Gen
import org.scalatest.{Assertion, BeforeAndAfterEach}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class MongoSourceSuite extends AsyncFlatSpec with MonixTaskSpec with Fixture with Matchers with BeforeAndAfterEach {

  override implicit val scheduler: Scheduler = Scheduler.io("mongo-source-suite")

  override def beforeEach() = {
    super.beforeEach()
    MongoDb.dropCollection(db, employeesColName).runSyncUnsafe()
    MongoDb.dropCollection(db, companiesColName).runSyncUnsafe()
  }

  s"aggregate" should  "aggregate with a match aggregation" in {
    val oldEmployees = genEmployeesWith(age = Some(55)).sample.get
    val youngEmployees = genEmployeesWith(age = Some(22)).sample.get
    for {
      _ <- MongoSingle.insertMany(employeesMongoCol, youngEmployees ++ oldEmployees)
      aggregation =  Aggregates.`match`(Filters.gt("age", 35))
      aggregated <- MongoSource.aggregate[Employee, Employee](employeesMongoCol, Seq(aggregation), classOf[Employee]).toListL
    } yield {
      aggregated.size shouldBe oldEmployees.size
    }

  }

  it should "aggregate with group by" in {
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get

    for {
      _ <- MongoSingle.insertMany(employeesMongoCol, employees)

      aggregation = Aggregates.group("group", Accumulators.avg("average", "$age"))
      aggregated <- MongoSource.aggregate[Employee](employeesMongoCol, Seq(aggregation)).toListL
    } yield {
      aggregated.head.getDouble("average") shouldBe (employees.map(_.age).sum.toDouble / employees.size)
    }

  }

  it should "pipes multiple aggregations" in {
    val e1 = genEmployeeWith(age = Some(55)).sample.get
    val e2 = genEmployeeWith(age = Some(65)).sample.get
    val e3 = genEmployeeWith(age = Some(22)).sample.get
    for {
      _ <- MongoSingle.insertMany(employeesMongoCol, List(e1, e2, e3))
      matchAgg = Aggregates.`match`(Filters.gt("age", 35))
      groupAgg = Aggregates.group("group", Accumulators.avg("average", "$age"))
      aggregated <- MongoSource.aggregate[Employee](employeesMongoCol, Seq(matchAgg, groupAgg)).toListL
    } yield {
      aggregated.head.getDouble("average") shouldBe 60
    }
  }

  it should "aggregate with unwind" in {
    val hobbies = List("reading", "running", "programming")
    val employee: Employee = genEmployeeWith(city = Some("Toronto"), activities = hobbies).sample.get
    for {
      _ <- MongoSingle.insertOne(employeesMongoCol, employee)
      filter = Aggregates.`match`(Filters.eq("city", "Toronto"))
      unwind = Aggregates.unwind("$activities")
      unwinded <- MongoSource.aggregate[Employee, UnwoundEmployee](employeesMongoCol, Seq(filter, unwind), classOf[UnwoundEmployee]).toListL
    } yield {
      unwinded.size shouldBe 3
      unwinded.map(_.activities) should contain theSameElementsAs hobbies
    }
  }

  it should "aggregate with unwind in new api" in {

    case class Person(name: String, age: Int, hobbies: Seq[String])
    case class UnwoundPerson(name: String, age: Int, hobbies: String)

    import org.mongodb.scala.bson.codecs.Macros._
    val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[Person], classOf[UnwoundPerson]))
    val hobbies =  List("reading", "running", "programming")
    val col = CollectionCodecRef("myDb", "persons", classOf[Person], codecRegistry)

    MongoConnection.create1(mongoEndpoint, col).use[Task, Assertion]{ operator =>
      for {
        _  <- MongoDb.dropCollection(db, "persons")
        _ <- operator.single.insertOne(Person("Mario", 32, hobbies))
        unwound <- {
          val filter = Aggregates.`match`(Filters.gte("age", 32))
          val unwind = Aggregates.unwind("$hobbies")
          operator.source.aggregate(Seq(filter, unwind), classOf[UnwoundPerson]).toListL
          /** Returns ->
            *  List(
            *   UnwoundPerson("Mario", 32, "reading"),
            *   UnwoundPerson("Mario", 32, "running"),
            *   UnwoundPerson("Mario", 32, "programming")
            *   )
            */
        }
        //
      } yield {
        unwound.size shouldBe 3
        unwound.map(_.hobbies) should contain theSameElementsAs hobbies
      }
    }
  }

  "count" should  "count all" in {
    val n = 10
    val employees = Gen.listOfN(n, genEmployee).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees) >>
    MongoSource.countAll(employeesMongoCol).asserting(_ shouldBe n)
  }

  it should  "count by filter" in {
    val n = 6
    val scottishEmployees = genEmployeesWith(city = Some("Edinburgh"), n = n).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.++(scottishEmployees)).sample.get
    val filer: Bson = Filters.eq("city", "Edinburgh")
    MongoSingle.insertMany(employeesMongoCol, employees) *>
      MongoSource.count(employeesMongoCol, filer).asserting{
        _ shouldBe scottishEmployees.size
      }
  }


  it should  "count with countOptions" in {
    val senegalEmployees = genEmployeesWith(city = Some("Dakar")).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.++(senegalEmployees)).sample.get
    val countOptions = new CountOptions().limit(senegalEmployees.size -1)
    val filer: Bson = Filters.eq("city", "Dakar")
    MongoSingle.insertMany(employeesMongoCol, employees) >>
     MongoSource.count(employeesMongoCol, filer, countOptions).asserting{
       _ shouldBe senegalEmployees.size - 1
     }
  }

  it should  "count 0 documents when on empty collections" in {
    MongoSource.countAll(employeesMongoCol).asserting(_ shouldBe 0L)
  }

  "distinct" should  "distinguish unique fields" in {
    //given
    val chineseEmployees = genEmployeesWith(city = Some("Shanghai")).sample.get
    val employees = Gen.nonEmptyListOf(genEmployee).map(_ ++ chineseEmployees).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees) *>
     MongoSource.distinct(employeesMongoCol, "city", classOf[String]).toListL.asserting { distinct =>
       assert(distinct.size < employees.size)
       distinct.filter(_.equals("Shanghai")).size shouldBe 1
     }
  }

  it should "findAll elements in a collection" in {
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees) *>
       MongoSource.findAll[Employee](employeesMongoCol).toListL.asserting{
         _ should contain theSameElementsAs employees
       }

  }

  it should  "find no elements" in {
    MongoSource.findAll[Employee](employeesMongoCol).toListL.asserting(_ shouldBe List.empty)
  }

  it should  "find filtered elements" in {
    val miamiEmployees = genEmployeesWith(city = Some("San Francisco")).sample.get
    val employees = Gen.nonEmptyListOf(genEmployee).map(_ ++ miamiEmployees).sample.get
    MongoSingle.insertMany(employeesMongoCol, employees) *>
      MongoSource.find[Employee](employeesMongoCol, Filters.eq("city", "San Francisco")).toListL.asserting{
        _ should contain theSameElementsAs miamiEmployees
      }
  }

  it should "be likewise available from within the resource usage" in {
    val employee = genEmployeeWith(name = Some("Pablo")).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.:+(employee)).sample.get
    val company = Company("myCompany", employees, 1000)

    MongoConnection.create1(mongoEndpoint, companiesCol).use[Task, Assertion]{ case CollectionOperator(_, source, single, _) =>
      for{
        _ <- single.insertOne(company)
        //two different ways to filter the same thing
        exists1 <- source.find(Filters.in("employees", employee)).nonEmptyL
        exists2 <- source.findAll.filter(_.name == company.name).map(_.employees.contains(employee)).headOrElseL(false)
      } yield ((exists1 && exists2) shouldBe true)
    }
  }

  "findOneAndDelete" should  "find one document by a given filter, delete it and return it" in {
    val n = 10
    val employees = genEmployeesWith(n = n, city = Some("Cracow")).sample.get

    val filter = Filters.eq("city", "Cracow")
   for {
     _ <- MongoSingle.insertMany(employeesMongoCol, employees)
     findAndDelete <- MongoSource.findOneAndDelete[Employee](employeesMongoCol, filter)
     findAll <- MongoSource.findAll[Employee](employeesMongoCol).toListL
   } yield {
     findAndDelete.isDefined shouldBe true
     findAndDelete.get.city shouldBe "Cracow"
     findAll.size shouldBe n - 1
   }
  }

  it should  "not find nor delete when filter didn't find matches" in {
    val filter = Filters.eq("city", "Cairo")
    MongoSource.findOneAndDelete[Employee](employeesMongoCol, filter).asserting{
      _.isDefined shouldBe false
    }
  }

  it should "be likewise available from within the resource usage" in {
    val n = 10
    val employees = genEmployeesWith(n = n, city = Some("Zurich")).sample.get
    for {
      _ <- MongoSingle.insertMany(employeesMongoCol, employees)
      filter = Filters.eq("city", "Zurich")
      findAndDelete <- MongoConnection.create1(mongoEndpoint, employeesCol).use(_.source.findOneAndDelete(filter))
      findAll <- MongoSource.findAll(employeesMongoCol).toListL
    } yield {
      findAndDelete.isDefined shouldBe true
      findAndDelete.get.city shouldBe "Zurich"
      findAll.size shouldBe n - 1
    }
  }

    "replaceOne" should "find and replace one single document" in {
    val employeeA = genEmployeeWith(name = Some("Beth")).sample.get
    val employeeB = genEmployeeWith(name = Some("Samantha")).sample.get

    for {
      _ <- MongoSingle.insertOne(employeesMongoCol, employeeA)
      findAndReplace <- MongoSource.findOneAndReplace(employeesMongoCol, Filters.eq("name", employeeA.name), employeeB)
      replacement <- MongoSource.find(employeesMongoCol, Filters.eq("name", employeeB.name)).headL
    } yield {
      findAndReplace.isDefined shouldBe true
      findAndReplace.get shouldBe employeeA
      replacement shouldBe employeeB
    }
  }

  it should "not find nor replace when filter didn't find matches" in {
    val employee = genEmployeeWith(name = Some("Alice")).sample.get
    val filter: Bson = Filters.eq("name", "Whatever") //it does not exist

    for {
      findAndReplace <- MongoSource.findOneAndReplace(employeesMongoCol, filter, employee)
      count <- MongoSource.count(employeesMongoCol, Filters.eq("name", "Alice"))
    } yield {
      findAndReplace.isDefined shouldBe false
      count shouldBe 0L
    }
  }

  it should "be likewise available from within the resource usage" in {
    val employeeA = genEmployeeWith(name = Some("Beth")).sample.get
    val employeeB = genEmployeeWith(name = Some("Samantha")).sample.get

    for {
      _ <- MongoSingle.insertOne(employeesMongoCol, employeeA)
      findOneAndReplace <- MongoConnection.create1(mongoEndpoint, employeesCol)
        .use(_.source.findOneAndReplace(Filters.eq("name", employeeA.name), employeeB))
      replacement <- MongoSource.find(employeesMongoCol, Filters.eq("name", employeeB.name)).headL
    } yield {
      findOneAndReplace.isDefined shouldBe true
      findOneAndReplace.get shouldBe employeeA
      replacement shouldBe employeeB
    }
  }

    "findOneAndUpdate" should "find a single document by a filter, update and return it" in {
    val employee = genEmployeeWith(name = Some("Glen")).sample.get
    val filter = Filters.eq("name", employee.name)

    for {
      _ <- MongoSingle.insertOne(employeesMongoCol, employee)
      update = Updates.inc("age", 1)
      findAndUpdate <- MongoSource.findOneAndUpdate(employeesMongoCol, filter, update)
      updated <- MongoSource.find(employeesMongoCol, filter).headL
    } yield {
      findAndUpdate.isDefined shouldBe true
      findAndUpdate.get shouldBe employee
      updated.age shouldBe employee.age + 1
    }
  }

  it should "not find nor update when filter didn't find matches" in {
    val filter: Bson = Filters.eq("name", "Isabelle") //it does not exist
    val update = Updates.inc("age", 1)

    MongoSource.findOneAndUpdate(employeesMongoCol, filter, update).asserting {
      _.isDefined shouldBe false
    }
  }

  it should "be likewise available from within the resource usage" in {
    val employee = genEmployeeWith(name = Some("Jack")).sample.get
    val filter = Filters.eq("name", employee.name)

    for {
      _ <- MongoSingle.insertOne(employeesMongoCol, employee)
      update = Updates.inc("age", 1)
      findAndUpdate <- MongoConnection.create1(mongoEndpoint, employeesCol)
        .use(_.source.findOneAndUpdate(filter, update))
      updated <- MongoSource.find(employeesMongoCol, filter).headL
    } yield {
      findAndUpdate.isDefined shouldBe true
      findAndUpdate.get shouldBe employee
      updated.age shouldBe employee.age + 1
    }
  }
}
