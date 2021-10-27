package monix.connect.mongodb

import cats.effect.Resource
import com.mongodb.client.model.{Filters, Updates}
import monix.connect.mongodb.client.{CollectionCodecRef, CollectionOperator, CollectionRef}
import monix.connect.mongodb.domain.{Tuple2F, Tuple3F, Tuple4F, Tuple5F, Tuple6F, Tuple7F, Tuple8F}
import monix.eval.Task
import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait MongoConnectionFixture extends Fixture {

  this: AsyncTestSuite with Matchers =>
    protected[this] def createConnectionTest2(
      makeResource: (
        CollectionRef[Employee],
        CollectionRef[Company]) => Resource[Task, Tuple2F[CollectionOperator, Employee, Company]]): Task[Assertion] = {
      val employee = genEmployee.sample.get
      val company = genCompany.sample.get
      val connection = makeResource(randomEmployeesColRef, randomCompaniesColRef)

      connection.use {
        case (
            CollectionOperator(employeeDb, employeeSource, employeeSingle, employeeSink),
            CollectionOperator(companyDb, companySource, companySingle, companySink)) =>
          for {
            r1 <- employeeSingle
              .insertOne(employee)
              .flatMap(_ => employeeSource.find(Filters.eq("name", employee.name)).headL)
            r2 <- companySingle
              .insertOne(company)
              .flatMap(_ => companySource.find(Filters.eq("name", company.name)).headL)
          } yield {
            r1 shouldBe employee
            r2 shouldBe company
          }
      }
    }

    protected[this] def abstractCreateConnectionTest3(
      makeResource: (
        CollectionRef[Company],
        CollectionRef[Employee],
        CollectionRef[Investor]) => Resource[Task, Tuple3F[CollectionOperator, Company, Employee, Investor]])
      : Task[Assertion] = {
      val newCompanyName = randomName("NewCompany")
      val oldCompanyName = randomName("OldCompany")
      val investorName1 = randomName("MyInvestor1")
      val investorName2 = randomName("MyInvestor2")

      val employees = List(Employee("Caroline", 21, "Barcelona", oldCompanyName))
      val oldCompany = Company(oldCompanyName, employees, 0)
      val investor1 = Investor(investorName1, 10001, List(oldCompany))
      val investor2 = Investor(investorName2, 20001, List(oldCompany))
      val newCompany = Company(newCompanyName, employees = List.empty, investment = 0)

      makeResource(randomCompaniesColRef, randomEmployeesColRef, randomInvestorsColRef).use {      case (
        CollectionOperator(_, companySource, companySingle, companySink),
        CollectionOperator(_, employeeSource, employeeSingle, employeeSink),
        CollectionOperator(_, investorSource, investorSingle, _)) =>
        for {
        _ <- investorSingle.insertMany(List(investor1, investor2))
        _ <- employeeSingle.insertMany(employees)
        _ <- companySingle.insertOne(oldCompany)
        _ <- companySingle.insertOne(newCompany)
              .delayResult(1.second)
        _ <- {
              employeeSource
                .find(Filters.eq("companyName", oldCompanyName)) //read employees from old company
                .bufferTimedAndCounted(2.seconds, 15)
                .map { employees =>
                  // pushes them into the new one
                  (Filters.eq("name", newCompanyName), Updates.pushEach("employees", employees.asJava))
                }
                .consumeWith(companySink.updateOne())
            }
            //aggregates all the
            investment <- investorSource.find(Filters.in("companies.name", oldCompanyName)).map(_.funds).sumL
            updateResult <- companySingle.updateMany(
              Filters.eq("name", newCompanyName),
              Updates.set("investment", investment))
            newCompany <- Task.sleep(1.second) >> companySource.find(Filters.eq("name", newCompanyName)).headL
          } yield {
            updateResult.wasAcknowledged shouldBe true
            updateResult.matchedCount shouldBe 1
           newCompany.employees should contain theSameElementsAs employees
            newCompany.investment shouldBe investor1.funds + investor2.funds
          }
      }
    }

    protected[this] def abstractCreateConnectionTest4(
      makeResource: Tuple4F[CollectionRef, Employee, Employee, Employee, Company] => Resource[
        Task,
        Tuple4F[CollectionOperator, Employee, Employee, Employee, Company]]
    ): Task[Assertion] = {
      val company = genCompany.sample.get
      val (employee1, employee2, employee3) = (genEmployee.sample.get, genEmployee.sample.get, genEmployee.sample.get)
      val connection = makeResource((randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomCompaniesColRef))

      connection.use {
        case (employees1, employees2, employees3, companies) =>
          for {
            r1 <- employees1.single
              .insertOne(employee1)
              .flatMap(_ => employees1.source.find(Filters.eq("name", employee1.name)).headL)
            r2 <- employees2.single
              .insertOne(employee2)
              .flatMap(_ => employees2.source.find(Filters.eq("name", employee2.name)).headL)
            r3 <- employees3.single
              .insertOne(employee3)
              .flatMap(_ => employees3.source.find(Filters.eq("name", employee3.name)).headL)
            r4 <- companies.single
              .insertOne(company)
              .flatMap(_ => companies.source.find(Filters.eq("name", company.name)).headL)
          } yield {
            r1 shouldBe employee1
            r2 shouldBe employee2
            r3 shouldBe employee3
            r4 shouldBe company
          }
      }
  }

  protected[this] def abstractCreateConnectionTest5(
    makeResource: Tuple5F[CollectionRef, Employee, Employee, Employee, Employee, Company] => Resource[
      Task,
      Tuple5F[CollectionOperator, Employee, Employee, Employee, Employee, Company]]): Task[Assertion] = {

    val company = genCompany.sample.get
    val (employee1, employee2, employee3, employee4) =
      (genEmployee.sample.get, genEmployee.sample.get, genEmployee.sample.get, genEmployee.sample.get)
    val connection = makeResource((randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomCompaniesColRef))

    connection.use {
      case (employees1, employees2, employees3, employees4, companies) =>
        for {
          r1 <- employees1.single
            .insertOne(employee1)
            .flatMap(_ => employees1.source.find(Filters.eq("name", employee1.name)).headL)
          r2 <- employees2.single
            .insertOne(employee2)
            .flatMap(_ => employees2.source.find(Filters.eq("name", employee2.name)).headL)
          r3 <- employees3.single
            .insertOne(employee3)
            .flatMap(_ => employees3.source.find(Filters.eq("name", employee3.name)).headL)
          r4 <- employees4.single
            .insertOne(employee4)
            .flatMap(_ => employees4.source.find(Filters.eq("name", employee4.name)).headL)
          r5 <- companies.single
            .insertOne(company)
            .flatMap(_ => companies.source.find(Filters.eq("name", company.name)).headL)
        } yield {
          r1 shouldBe employee1
          r2 shouldBe employee2
          r3 shouldBe employee3
          r4 shouldBe employee4
          r5 shouldBe company
        }
    }

  }

  protected[this] def abstractCreateConnectionTest6(
    makeResource: Tuple6F[CollectionRef, Employee, Employee, Employee, Employee, Employee, Company] => Resource[
      Task,
      Tuple6F[CollectionOperator, Employee, Employee, Employee, Employee, Employee, Company]]): Task[Assertion] = {

    val company = genCompany.sample.get
    val (employee1, employee2, employee3, employee4, employee5) =
      (
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get)
    val connection = makeResource((randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomCompaniesColRef))

    connection.use {
      case (employees1, employees2, employees3, employees4, employees5, companies) =>
        for {
          r1 <- employees1.single
            .insertOne(employee1)
            .flatMap(_ => employees1.source.find(Filters.eq("name", employee1.name)).headL)
          r2 <- employees2.single
            .insertOne(employee2)
            .flatMap(_ => employees2.source.find(Filters.eq("name", employee2.name)).headL)
          r3 <- employees3.single
            .insertOne(employee3)
            .flatMap(_ => employees3.source.find(Filters.eq("name", employee3.name)).headL)
          r4 <- employees4.single
            .insertOne(employee4)
            .flatMap(_ => employees4.source.find(Filters.eq("name", employee4.name)).headL)
          r5 <- employees5.single
            .insertOne(employee5)
            .flatMap(_ => employees5.source.find(Filters.eq("name", employee5.name)).headL)
          r6 <- companies.single
            .insertOne(company)
            .flatMap(_ => companies.source.find(Filters.eq("name", company.name)).headL)
        } yield {
          r1 shouldBe employee1
          r2 shouldBe employee2
          r3 shouldBe employee3
          r4 shouldBe employee4
          r5 shouldBe employee5
          r6 shouldBe company
        }
    }

  }

  protected[this] def abstractCreateConnectionTest7(makeResource: Tuple7F[
    CollectionRef,
    Employee,
    Employee,
    Employee,
    Employee,
    Employee,
    Employee,
    Company] => Resource[
    Task,
    Tuple7F[CollectionOperator, Employee, Employee, Employee, Employee, Employee, Employee, Company]]): Task[Assertion] = {
    //given
    val company = genCompany.sample.get
    val (employee1, employee2, employee3, employee4, employee5, employee6) =
      (
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get)
    val connection =
      makeResource((randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomEmployeesColRef, randomCompaniesColRef))

    connection.use {
      case (employees1, employees2, employees3, employees4, employees5, employees6, companies) =>
        for {
          r1 <- employees1.single
            .insertOne(employee1)
            .flatMap(_ => employees1.source.find(Filters.eq("name", employee1.name)).headL)
          r2 <- employees2.single
            .insertOne(employee2)
            .flatMap(_ => employees2.source.find(Filters.eq("name", employee2.name)).headL)
          r3 <- employees3.single
            .insertOne(employee3)
            .flatMap(_ => employees3.source.find(Filters.eq("name", employee3.name)).headL)
          r4 <- employees4.single
            .insertOne(employee4)
            .flatMap(_ => employees4.source.find(Filters.eq("name", employee4.name)).headL)
          r5 <- employees5.single
            .insertOne(employee5)
            .flatMap(_ => employees5.source.find(Filters.eq("name", employee5.name)).headL)
          r6 <- employees6.single
            .insertOne(employee6)
            .flatMap(_ => employees6.source.find(Filters.eq("name", employee6.name)).headL)
          r7 <- companies.single
            .insertOne(company)
            .flatMap(_ => companies.source.find(Filters.eq("name", company.name)).headL)
        } yield {
          r1 shouldBe employee1
          r2 shouldBe employee2
          r3 shouldBe employee3
          r4 shouldBe employee4
          r5 shouldBe employee5
          r6 shouldBe employee6
          r7 shouldBe company
        }
    }
  }

  protected[this] def abstractCreateConnectionTest8(
    makeResource: Tuple8F[
      CollectionRef,
      Employee,
      Employee,
      Employee,
      Employee,
      Employee,
      Employee,
      Employee,
      Company] => Resource[
      Task,
      Tuple8F[CollectionOperator, Employee, Employee, Employee, Employee, Employee, Employee, Employee, Company]])
    : Task[Assertion] = {
    val company = genCompany.sample.get
    val (employee1, employee2, employee3, employee4, employee5, employee6, employee7) =
      (
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get,
        genEmployee.sample.get)
    val connection = makeResource((
      randomEmployeesColRef,
      randomEmployeesColRef,
      randomEmployeesColRef,
      randomEmployeesColRef,
      randomEmployeesColRef,
      randomEmployeesColRef,
      randomEmployeesColRef,
      randomCompaniesColRef))

    connection.use {
      case (employees1, employees2, employees3, employees4, employees5, employees6, employees7, companies) =>
        for {
          r1 <- employees1.single
            .insertOne(employee1)
            .flatMap(_ => employees1.source.find(Filters.eq("name", employee1.name)).headL)
          r2 <- employees2.single
            .insertOne(employee2)
            .flatMap(_ => employees2.source.find(Filters.eq("name", employee2.name)).headL)
          r3 <- employees3.single
            .insertOne(employee3)
            .flatMap(_ => employees3.source.find(Filters.eq("name", employee3.name)).headL)
          r4 <- employees4.single
            .insertOne(employee4)
            .flatMap(_ => employees4.source.find(Filters.eq("name", employee4.name)).headL)
          r5 <- employees5.single
            .insertOne(employee5)
            .flatMap(_ => employees5.source.find(Filters.eq("name", employee5.name)).headL)
          r6 <- employees6.single
            .insertOne(employee6)
            .flatMap(_ => employees6.source.find(Filters.eq("name", employee6.name)).headL)
          r7 <- employees7.single
            .insertOne(employee7)
            .flatMap(_ => employees7.source.find(Filters.eq("name", employee7.name)).headL)
          r8 <- companies.single
            .insertOne(company)
            .flatMap(_ => companies.source.find(Filters.eq("name", company.name)).headL)
        } yield {
          r1 shouldBe employee1
          r2 shouldBe employee2
          r3 shouldBe employee3
          r4 shouldBe employee4
          r5 shouldBe employee5
          r6 shouldBe employee6
          r7 shouldBe employee7
          r8 shouldBe company
        }
    }
  }
}

