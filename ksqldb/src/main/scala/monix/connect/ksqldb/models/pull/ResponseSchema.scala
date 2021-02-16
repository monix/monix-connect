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

package monix.connect.ksqldb.models.pull

import tethys._
import tethys.derivation.semiauto._

/**
  * Schema for the pull query result
  * @param queryId unique ID of a query
  * @param columnNames the names of the columns
  * @param columnTypes The types of the columns
  *
  * @author Andrey Romanov
  */
case class ResponseSchema(
  queryId: Option[String],
  columnNames: List[String],
  columnTypes: List[String]
)

object ResponseSchema {

  implicit val reader: JsonReader[ResponseSchema] = jsonReader[ResponseSchema]
  implicit val writer: JsonWriter[ResponseSchema] = jsonWriter[ResponseSchema]

}
