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

  s"aggregate" should  "aggregate with a match aggregation" in {
    val oldEmployees = genEmployeesWith(age = Some(55)).sample.get
    val youngEmployees = genEmployeesWith(age = Some(22)).sample.get
    val aggregation = Aggregates.`match`(Filters.gt("age", 35))
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
     operator.single.insertMany(youngEmployees ++ oldEmployees) >>
       operator.source.aggregate[Employee](Seq(aggregation), classOf[Employee]).toListL
      }.asserting {_.size shouldBe oldEmployees.size }

  }

  it should "aggregate with group by" in {
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get
    val aggregation = Aggregates.group("group", Accumulators.avg("average", "$age"))

    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) >>
        operator.source.aggregate(Seq(aggregation)).toListL
    }.asserting { _.head.getDouble("average") shouldBe (employees.map(_.age).sum.toDouble / employees.size)
    }
  }

  it should "pipes multiple aggregations" in {
    val e1 = genEmployeeWith(age = Some(55)).sample.get
    val e2 = genEmployeeWith(age = Some(65)).sample.get
    val e3 = genEmployeeWith(age = Some(22)).sample.get
    val matchAgg = Aggregates.`match`(Filters.gt("age", 35))
    val groupAgg = Aggregates.group("group", Accumulators.avg("average", "$age"))
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(List(e1, e2, e3)) >>
        operator.source.aggregate(Seq(matchAgg, groupAgg)).toListL
    }.asserting{ _.head.getDouble("average") shouldBe 60
    }
  }

  it should "aggregate with unwind" in {
    val hobbies = List("reading", "running", "programming")
    val employee: Employee = genEmployeeWith(city = Some("Toronto"), activities = hobbies).sample.get
    val filter = Aggregates.`match`(Filters.eq("city", "Toronto"))
    val unwind = Aggregates.unwind("$activities")
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertOne(employee) >>
        operator.source.aggregate[UnwoundEmployee](Seq(filter, unwind), classOf[UnwoundEmployee]).toListL
    }.asserting { unwinded =>
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
    val col = CollectionCodecRef(dbName, randomName("persons"), classOf[Person], codecRegistry)

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
      } yield {
        unwound.size shouldBe 3
        unwound.map(_.hobbies) should contain theSameElementsAs hobbies
      }
    }
  }

  "count" should  "count all" in {
    val n = 10
    val employees = Gen.listOfN(n, genEmployee).sample.get
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) >>
        operator.source.countAll()
    }.asserting(_ shouldBe n)
  }

  it should  "count by filter" in {
    val n = 6
    val scottishEmployees = genEmployeesWith(city = Some("Edinburgh"), n = n).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.++(scottishEmployees)).sample.get
    val filer: Bson = Filters.eq("city", "Edinburgh")
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) *>
        operator.source.count(filer)
    }.asserting {
      _ shouldBe scottishEmployees.size
    }
  }


  it should  "count with countOptions" in {
    val senegalEmployees = genEmployeesWith(city = Some("Dakar")).sample.get
    val employees = Gen.listOfN(10, genEmployee).map(l => l.++(senegalEmployees)).sample.get
    val countOptions = new CountOptions().limit(senegalEmployees.size - 1)
    val filer: Bson = Filters.eq("city", "Dakar")
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) >>
        operator.source.count(filer, countOptions)
    }.asserting {
      _ shouldBe senegalEmployees.size - 1
    }
  }

  it should  "count 0 documents when on empty collections" in {
    MongoSource.countAll(randomEmployeesMongoCol).asserting(_ shouldBe 0L)
  }

  "distinct" should  "distinguish unique fields" in {
    val chineseEmployees = genEmployeesWith(city = Some("Shanghai")).sample.get
    val employees = Gen.nonEmptyListOf(genEmployee).map(_ ++ chineseEmployees).sample.get
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) *>
        operator.source.distinct("city", classOf[String]).toListL
    }.asserting { distinct =>
      assert(distinct.size < employees.size)
      distinct.filter(_.equals("Shanghai")).size shouldBe 1
    }
  }

  it should "findAll elements in a collection" in {
    val employees = Gen.nonEmptyListOf(genEmployee).sample.get
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) *>
        operator.source.findAll.toListL
    }.asserting {
      _ should contain theSameElementsAs employees
    }
  }

  it should  "find no elements" in {
    MongoSource.findAll[Employee](randomEmployeesMongoCol).toListL.asserting(_ shouldBe List.empty)
  }

  it should  "find filtered elements" in {
    val miamiEmployees = genEmployeesWith(city = Some("San Francisco")).sample.get
    val employees = Gen.nonEmptyListOf(genEmployee).map(_ ++ miamiEmployees).sample.get
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use { operator =>
      operator.single.insertMany(employees) *>
        operator.source.find(Filters.eq("city", "San Francisco")).toListL
    }.asserting {
      _ should contain theSameElementsAs miamiEmployees
    }
  }

  it should "be likewise available from within the resource usage" in {
    val employee = genEmployeeWith(name = Some("Pablo")).sample.get
    val employees =  Gen.listOfN(10, genEmployee).map(l => l.:+(employee)).sample.get
    val company = Company("myCompany", employees, 1000)
    MongoConnection.create1(mongoEndpoint, randomCompaniesColRef).use[Task, Assertion]{ case CollectionOperator(_, source, single, _) =>
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
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertMany(employees)
        findAndDelete <- operator.source.findOneAndDelete(filter)
        findAll <- operator.source.findAll.toListL
      } yield {
        findAndDelete.isDefined shouldBe true
        findAndDelete.get.city shouldBe "Cracow"
        findAll.size shouldBe n - 1
      }
    }
  }

  it should  "not find nor delete when filter didn't find matches" in {
    val filter = Filters.eq("city", "Cairo")
    MongoSource.findOneAndDelete[Employee](randomEmployeesMongoCol, filter).asserting{
      _.isDefined shouldBe false
    }
  }

  it should "be likewise available from within the resource usage" in {
    val n = 10
    val employees = genEmployeesWith(n = n, city = Some("Zurich")).sample.get
    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertMany(employees)
        filter = Filters.eq("city", "Zurich")
        findAndDelete <-operator.source.findOneAndDelete(filter)
        findAll <- operator.source.findAll.toListL
      } yield {
        findAndDelete.isDefined shouldBe true
        findAndDelete.get.city shouldBe "Zurich"
        findAll.size shouldBe n - 1
      }
    }
  }

    "replaceOne" should "find and replace one single document" in {
    val employeeA = genEmployeeWith(name = Some("Beth")).sample.get
    val employeeB = genEmployeeWith(name = Some("Samantha")).sample.get

      MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
        for {
          _ <- operator.single.insertOne(employeeA)
          findAndReplace <- operator.source.findOneAndReplace(Filters.eq("name", employeeA.name), employeeB)
          replacement <- operator.source.find(Filters.eq("name", employeeB.name)).headL
        } yield {
          findAndReplace.isDefined shouldBe true
          findAndReplace.get shouldBe employeeA
          replacement shouldBe employeeB
        }
      }
  }

  it should "not find nor replace when filter didn't find matches" in {
    val employee = genEmployeeWith(name = Some("Alice")).sample.get
    val filter: Bson = Filters.eq("name", "Whatever") //it does not exist

    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        findAndReplace <- operator.source.findOneAndReplace(filter, employee)
        count <- operator.source.count(Filters.eq("name", "Alice"))
      } yield {
        findAndReplace.isDefined shouldBe false
        count shouldBe 0L
      }
    }
  }

  it should "be likewise available from within the resource usage" in {
    val employeeA = genEmployeeWith(name = Some("Beth")).sample.get
    val employeeB = genEmployeeWith(name = Some("Samantha")).sample.get

    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertOne(employeeA)
        findOneAndReplace <- operator.source.findOneAndReplace(Filters.eq("name", employeeA.name), employeeB)
        replacement <- operator.source.find(Filters.eq("name", employeeB.name)).headL
      } yield {
        findOneAndReplace.isDefined shouldBe true
        findOneAndReplace.get shouldBe employeeA
        replacement shouldBe employeeB
      }
    }
  }

    "findOneAndUpdate" should "find a single document by a filter, update and return it" in {
      val employee = genEmployeeWith(name = Some("Glen")).sample.get
      val filter = Filters.eq("name", employee.name)

      MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
        for {
          _ <- operator.single.insertOne(employee)
          update = Updates.inc("age", 1)
          findAndUpdate <- operator.source.findOneAndUpdate(filter, update)
          updated <- operator.source.find(filter).headL
        } yield {
          findAndUpdate.isDefined shouldBe true
          findAndUpdate.get shouldBe employee
          updated.age shouldBe employee.age + 1
        }
      }
  }

  it should "not find nor update when filter didn't find matches" in {
    val filter: Bson = Filters.eq("name", "Isabelle") //it does not exist
    val update = Updates.inc("age", 1)

    MongoSource.findOneAndUpdate(randomEmployeesMongoCol, filter, update).asserting {
      _.isDefined shouldBe false
    }
  }

  it should "be likewise available from within the resource usage" in {
    val employee = genEmployeeWith(name = Some("Jack")).sample.get
    val filter = Filters.eq("name", employee.name)

    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertOne(employee)
        update = Updates.inc("age", 1)
        findAndUpdate <- operator.source.findOneAndUpdate(filter, update)
        updated <- operator.source.find(filter).headL
      } yield {
        findAndUpdate.isDefined shouldBe true
        findAndUpdate.get shouldBe employee
        updated.age shouldBe employee.age + 1
      }
    }
  }

  "findOne" should "find first encountered item that matches the filter" in {
    val employeeGlen1 = genEmployeeWith(name = Some("Glen"), age = Some(30)).sample.get
    val employeeGlen2 = genEmployeeWith(name = Some("Glen"), age = Some(40)).sample.get
    val employeeGlen3 = genEmployeeWith(name = Some("Glen"), age = Some(50)).sample.get
    val employeeMike1 = genEmployeeWith(name = Some("Mike"), age = Some(30)).sample.get
    val employeeMike2 = genEmployeeWith(name = Some("Mike"), age = Some(40)).sample.get

    val filter = Filters.eq("name", "Glen")

    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertMany(List(employeeMike1, employeeGlen1, employeeGlen2, employeeMike2, employeeGlen3))
        findResult <- operator.source.findOne(filter)
      } yield {
        findResult.isDefined shouldBe true
        findResult.get.name shouldBe "Glen"
        List(employeeGlen1, employeeGlen2, employeeGlen3) should contain (findResult.get)
      }
    }
  }

  it should "not find result when filter didn't find matches" in {
    val employeeGlen1 = genEmployeeWith(name = Some("Glen"), age = Some(30)).sample.get
    val employeeGlen2 = genEmployeeWith(name = Some("Glen"), age = Some(40)).sample.get

    val filter = Filters.eq("name", "John")

    MongoConnection.create1(mongoEndpoint, randomEmployeesColRef).use[Task, Assertion] { operator =>
      for {
        _ <- operator.single.insertMany(List(employeeGlen1, employeeGlen2))
        findResult <- operator.source.findOne(filter)
      } yield {
        findResult.isDefined shouldBe false
      }
    }
  }
}
