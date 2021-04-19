/*
 * Copyright (c) 2020-2021 by The Monix Connect Project Developers.
 * See the project homepage at: https://connect.monix.io
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

package monix.connect.mongodb.client

import org.bson.Document
import org.bson.codecs.configuration.CodecProvider

trait CollectionRef[+Doc] {
  val database: String
  val collection: String
}

/**
  * Represents the reference to a collection with a custom codec.
  *
  * ==Example==
  * {{{
  *   case class Employee(name: String, age: Int, city: String, companyName: String, activities: List[String])
  * }}}
  */
final case class CollectionCodecRef[Doc](
  database: String,
  collection: String,
  clazz: Class[Doc],
  codecProviders: CodecProvider*)
  extends CollectionRef[Doc]

final case class CollectionDocumentRef(database: String, collection: String) extends CollectionRef[Document]
