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

package monix.connect.ksqldb.models.query

import tethys._

/**
  * Case class, which represents most of queries for '/query' endpoint
  *
  * @param input Input KSQL query (single)
  * @param properties Map of settings for input KSQL query
  *
  * @author Andrey Romanov
  */
case class Request(input: String, properties: Map[String, String])

object Request {

  implicit val reader: JsonReader[Request] = JsonReader.builder
    .addField[String]("ksql")
    .addField[Map[String, String]]("streamsProperties")
    .buildReader(Request.apply)

  implicit val writer: JsonWriter[Request] = JsonWriter
    .obj[Request]
    .addField("ksql")(_.input)
    .addField("streamsProperties")(_.properties)

}
