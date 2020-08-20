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

import com.mongodb.reactivestreams.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.scalacheck.Gen
import org.mongodb.scala.bson.codecs.Macros._

trait Fixture {

  case class Employee(name: String, age: Int, city: String, activities: List[String] = List.empty)
  case class UnwoundEmployee(name: String, age: Int, city: String, activities: String)
  case class Company(name: String, nEmployees: Int)

  val codecRegistry = fromRegistries(fromProviders(classOf[Employee], classOf[UnwoundEmployee]), DEFAULT_CODEC_REGISTRY)

  protected val client: MongoClient = MongoClients.create(s"mongodb://localhost:27017")

  val db: MongoDatabase = client.getDatabase("mydb")
  val collectionName = "myCollection"
  val col: MongoCollection[Employee] = db.getCollection("myCollection", classOf[Employee])
    .withCodecRegistry(codecRegistry)

  protected val genNonEmptyStr = Gen.nonEmptyListOf(Gen.alphaLowerChar).map(_.mkString.take(10))

  val genEmployee = for {
    name <- Gen.nonEmptyListOf(Gen.alphaChar)
    age <- Gen.chooseNum(16, 65)
    age <- Gen.chooseNum(16, 65)
    city <- Gen.nonEmptyListOf(Gen.alphaChar)
  } yield Employee(name.mkString.take(8), age, city.mkString.take(8), List.empty)

  def genEmployeeWith(name: Option[String] = None, age: Option[Int] = None, city: Option[String] = None, activities: List[String] = List.empty) = {
    for {
      name <- if(name.isEmpty) Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString.take(8)) else Gen.const(name.get)
      age <-  if(age.isEmpty) Gen.chooseNum(16, 65) else Gen.const(age.get)
      city <- if(city.isEmpty) Gen.nonEmptyListOf(Gen.alphaChar).map(_.mkString.take(8)) else Gen.const(city.get)
    } yield Employee(name, age, city, activities)
  }

  def genEmployeesWith(name: Option[String] = None, age: Option[Int] = None, city: Option[String] = None, n: Int = 5, activities: List[String] = List.empty) =
    Gen.listOfN(n, genEmployeeWith(name, age, city, activities))
  
  def docNameFilter(name: String) = Document.parse(s"{'name':'$name'}")
  def nationalityDocument(nationality: String) = Document.parse(s"{'city':'$nationality'}")
  def employeeDocument(name: String, age: Int, city: String) = Document.parse(s"{'name':'$name', 'name':'$name'}")

}
