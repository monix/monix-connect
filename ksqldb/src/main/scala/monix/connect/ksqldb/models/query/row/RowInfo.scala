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

package monix.connect.ksqldb.models.query.row

import tethys._
import tethys.derivation.semiauto._

/**
  * Response with data from KSQL query
  *
  * @param row row with data
  * @param errorMessage error message in case of fail, in success case - null
  * @param finalMessage possible final message
  *
  * @author Andrey Romanov
  */
case class RowInfo(row: Data, errorMessage: Option[String], finalMessage: Option[String])

object RowInfo {

  implicit val reader: JsonReader[RowInfo] = jsonReader[RowInfo]
  implicit val writer: JsonWriter[RowInfo] = jsonWriter[RowInfo]

}
