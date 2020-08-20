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

import org.bson.BsonValue
import org.scalacheck.Gen
import org.mockito.MockitoSugar.mock
import org.mongodb.scala.bson.BsonString

trait TestFixture {

  case class Employee(name: String, age: Int, city: String, activities: List[String] = List.empty)

  val bsonValue = mock[BsonValue]

  val genEmployee = for {
    name        <- Gen.nonEmptyListOf(Gen.alphaChar)
    age         <- Gen.chooseNum(16, 65)
    nationality <- Gen.nonEmptyListOf(Gen.alphaChar)
  } yield Employee(name.mkString.take(8), age, nationality.mkString.take(8), List.empty)

}
