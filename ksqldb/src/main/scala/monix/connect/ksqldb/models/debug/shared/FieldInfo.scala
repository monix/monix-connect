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

/**
  * Object for representing the information about field from data
  * @param name title of the field
  * @param schema schema representation of field
  *
  * @author Andrey Romanov
  */
case class FieldInfo(name: String, schema: SchemaInfo)

object FieldInfo {

  implicit val reader: JsonReader[FieldInfo] = JsonReader.builder
    .addField[String]("name")
    .addField[SchemaInfo]("schema")
    .buildReader(FieldInfo.apply)

  implicit val writer: JsonWriter[FieldInfo] = JsonWriter
    .obj[FieldInfo]
    .addField("name")(_.name)
    .addField("schema")(_.schema)

}
