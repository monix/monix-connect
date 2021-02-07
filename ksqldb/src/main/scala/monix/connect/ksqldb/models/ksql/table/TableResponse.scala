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

package monix.connect.ksqldb.models.ksql.table

import tethys._

/**
  * Class for representing `LIST TABLES` and `SHOW TABLES` results
  *
  * @param statement source statment
  * @param tables sequence of tables
  *
  * @author Andrey Romanov
  */
case class TableResponse(statement: String, tables: Seq[TableInfo])

object TableResponse {

  implicit val reader: JsonReader[TableResponse] = JsonReader.builder
    .addField[String]("statementText")
    .addField[Seq[TableInfo]]("tables")
    .buildReader(TableResponse.apply)

  implicit val writer: JsonWriter[TableResponse] = JsonWriter
    .obj[TableResponse]
    .addField("statementText")(_.statement)
    .addField("tables")(_.tables)

}
