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

import com.mongodb.{MongoClientSettings, ServerAddress}
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import monix.connect.mongodb.client.{CollectionCodecRef, CollectionDocumentRef, CollectionRef}
import org.bson.Document
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.scalacheck.Gen
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY

import scala.jdk.CollectionConverters._

trait Fixture {

  case class Employee(name: String, age: Int, city: String, companyName: String = "x", activities: List[String] = List.empty)
  case class UnwoundEmployee(name: String, age: Int, city: String, activities: String)
  case class Investor(name: String, funds: Int, companies: List[Company])
  case class Company(name: String, employees: List[Employee], investment: Double)

  val provider: CodecProvider = classOf[Employee]
  val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[Employee], classOf[UnwoundEmployee], classOf[Company], classOf[Investor]), DEFAULT_CODEC_REGISTRY)

  val mongoEndpoint = "mongodb://localhost:27017"
  protected val client: MongoClient = MongoClients.create(mongoEndpoint)
  val mongoClientSettings =
    MongoClientSettings
      .builder()
      .applyToClusterSettings(builder => builder.hosts(List(new ServerAddress("localhost", 27017)).asJava))
      .build()

  def randomName(prefix: String): String = Gen.uuid.map(prefix + _.toString.take(10)).sample.get

  def randomDbName: String = randomName("myDb-")
  def randomEmployeesColName: String = randomName("employees-")
  def randomCompaniesColName: String = randomName("companies-")
  def randomInvestorsColName: String = randomName("investors-")
  def randomBsonColName: String = randomName("bson-")

  val dbName = randomName("myDb")
  val db: MongoDatabase = client.getDatabase(dbName)
  val employeesColName = randomName("employees")
  val companiesColName = randomName("companies")
  val investorsColName = randomName("investors")
  val bsonColName = randomName("bson")

  def randomEmployeesMongoCol: MongoCollection[Employee] = db.getCollection(randomEmployeesColName, classOf[Employee])
    .withCodecRegistry(codecRegistry)
  val employeesMongoCol: MongoCollection[Employee] = randomEmployeesMongoCol
  def randomCompaniesMongoCol: MongoCollection[Company] = db.getCollection(randomCompaniesColName, classOf[Company])
    .withCodecRegistry(codecRegistry)
  val companiesMongoCol = randomCompaniesMongoCol
  def randomInvestorsMongoCol: MongoCollection[Investor] = db.getCollection(randomInvestorsColName, classOf[Investor])
    .withCodecRegistry(codecRegistry)
  val investorsMongoCol: MongoCollection[Investor] = randomInvestorsMongoCol

  def randomEmployeesColRef: CollectionCodecRef[Employee] = CollectionCodecRef(dbName, randomEmployeesColName, classOf[Employee], createCodecProvider[Employee](), createCodecProvider[UnwoundEmployee])
  def randomCompaniesColRef: CollectionCodecRef[Company] = CollectionCodecRef(dbName, randomCompaniesColName, classOf[Company], createCodecProvider[Company](), createCodecProvider[Employee]())
  def randomInvestorsColRef: CollectionCodecRef[Investor] = CollectionCodecRef(dbName, randomInvestorsColName, classOf[Investor], createCodecProvider[Investor](), createCodecProvider[Company](), createCodecProvider[Employee]())
  def randomBsonColRef: CollectionRef[Document] = CollectionDocumentRef(
    randomDbName,
    randomBsonColName)

  val employeesCol = CollectionCodecRef(dbName, randomEmployeesColName, classOf[Employee], createCodecProvider[Employee]())
  val companiesCol = CollectionCodecRef(dbName, randomCompaniesColName, classOf[Company], createCodecProvider[Company](), createCodecProvider[Employee]())


  protected val genNonEmptyStr = Gen.identifier.map(_.take(10))

  val genInvestor = for {
    name <- Gen.identifier
    funds <- Gen.chooseNum(1, 100000)
    companies <- Gen.listOf(genCompany)
  } yield Investor(name.mkString.take(8), funds, companies)

  val genEmployee = for {
    name <- Gen.identifier
    age <- Gen.chooseNum(16, 65)
    city <- Gen.identifier
    company <- Gen.identifier
  } yield Employee(name.mkString.take(8), age, city.mkString.take(8), company, List.empty)

  val genCompany = for {
    name <- Gen.identifier
    employees <-  Gen.listOf(genEmployee)
  } yield Company(name, employees, 0)

  def genEmployeeWith(name: Option[String] = None, age: Option[Int] = None, city: Option[String] = None, companyName: Option[String] = None, activities: List[String] = List.empty) = {
    for {
      name <- if(name.isEmpty) Gen.identifier.map(_.take(8)) else Gen.const(name.get)
      age <-  if(age.isEmpty) Gen.chooseNum(16, 65) else Gen.const(age.get)
      city <- if(city.isEmpty) Gen.identifier.map(_.take(8)) else Gen.const(city.get)
      company <- if(companyName.isEmpty) Gen.identifier.map(_.take(8)) else Gen.const(companyName.get)
    } yield Employee(name, age, city, company, activities)
  }

  def genEmployeesWith(name: Option[String] = None, age: Option[Int] = None, city: Option[String] = None, companyName: Option[String] = None, n: Int = 5, activities: List[String] = List.empty) =
    Gen.listOfN(n, genEmployeeWith(name, age, city, companyName, activities))
  
  def docNameFilter(name: String) = Document.parse(s"{'name':'$name'}")
  def nationalityDocument(nationality: String) = Document.parse(s"{'city':'$nationality'}")
  def employeeDocument(name: String, age: Int, city: String) = Document.parse(s"{'name':'$name', 'name':'$name'}")

}
