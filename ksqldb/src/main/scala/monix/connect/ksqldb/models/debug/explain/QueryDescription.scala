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

package monix.connect.ksqldb.models.debug.explain

import tethys._
import tethys.derivation.semiauto._

import monix.connect.ksqldb.models.debug.shared.FieldInfo

/**
  * Case class for the definition of "EXPLAIN" query result
  * @param statementText The ksqlDB statement for which the query being explained is running.
  * @param fields A list of field objects that describes each field in the query output.
  * @param sources The streams and tables being read by the query.
  * @param sinks The streams and tables being written to by the query.
  * @param executionPlan They query execution plan.
  * @param topology The Kafka Streams topology that the query is running.
  *
  * @author Andrey Romanov
  */
case class QueryDescription(
  statementText: String,
  fields: List[FieldInfo],
  sources: List[String],
  sinks: List[String],
  executionPlan: String,
  topology: String
)

object QueryDescription {

  implicit val reader: JsonReader[QueryDescription] = jsonReader[QueryDescription]
  implicit val writer: JsonWriter[QueryDescription] = jsonWriter[QueryDescription]

}

/**
  * Case class for holding the "DESCRIBE" result
  * @param queryDescription result of "explain" query
  * @param overriddenProperties The property overrides that the query is running with.
  *
  * @author Andrey Romanov
  */
case class ExplainResult(
  queryDescription: QueryDescription,
  overriddenProperties: Map[String, String]
)

object ExplainResult {

  implicit val reader: JsonReader[ExplainResult] = jsonReader[ExplainResult]
  implicit val writer: JsonWriter[ExplainResult] = jsonWriter[ExplainResult]

}
