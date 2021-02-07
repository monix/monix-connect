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
  * Class for information about retrieved queries
  *
  * @param query statement, which started the query
  * @param sinks streams and tables for query results
  * @param id query ID
  *
  * @author Andrey Romanov
  */
case class QueryInfo(query: String, sinks: String, id: String)

object QueryInfo {

  implicit val reader: JsonReader[QueryInfo] = JsonReader.builder
    .addField[String]("queryString")
    .addField[String]("sinks")
    .addField[String]("id")
    .buildReader(QueryInfo.apply)

  implicit val writer: JsonWriter[QueryInfo] = JsonWriter
    .obj[QueryInfo]
    .addField("queryString")(_.query)
    .addField("sinks")(_.sinks)
    .addField("id")(_.id)

}
