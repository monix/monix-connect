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

package monix.connect.ksqldb.models.debug.shared

import tethys._
import tethys.derivation.semiauto._

/**
  * Schema representation for the data
  * @param `type`  data type
  * @param memberSchema inner schema , if the data type is MAP or ARRAY, otherwise - None
  * @param fields list of field objects if the data type is STRUCT, otherwise - None
  *
  * @author Andrey Romanov
  */
case class SchemaInfo(
  `type`: String,
  memberSchema: Option[SchemaInfo],
  fields: Option[List[FieldInfo]]
)

object SchemaInfo {

  implicit val reader: JsonReader[SchemaInfo] = jsonReader[SchemaInfo]
  implicit val writer: JsonWriter[SchemaInfo] = jsonWriter[SchemaInfo]

}
