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

package monix.connect.ksqldb.models.ksql.query

import tethys._

/**
  * Class for representing `LIST QUERIES` and `SHOW QUERIES` results
  *
  * @param statement source statement
  * @param queries sequence of queries
  *
  * @author Andrey Romanov
  */
case class QueryResponse(statement: String, queries: Seq[QueryInfo])

object QueryResponse {

  implicit val reader: JsonReader[QueryResponse] = JsonReader.builder
    .addField[String]("statementText")
    .addField[Seq[QueryInfo]]("queries")
    .buildReader(QueryResponse.apply)

  implicit val writer: JsonWriter[QueryResponse] = JsonWriter
    .obj[QueryResponse]
    .addField("statementText")(_.statement)
    .addField("queries")(_.queries)

}
