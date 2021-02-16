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

package monix.connect.ksqldb.models.debug.describe

import tethys._
import tethys.derivation.semiauto._

import monix.connect.ksqldb.models.debug.shared.FieldInfo

/**
  * Case class for the definition of "DESCRIBE" query result
  *
  * @param name name of table or stream to describe
  * @param readQueries The queries reading from the stream or table.
  * @param writeQueries The queries writing into the stream or table
  * @param fields A list of field objects that describes each field in the stream/table.
  * @param `type` STREAM or TABLE
  * @param key The name of the key column
  * @param timestamp The name of the timestamp column.
  * @param format The serialization format of the data in the stream or table. One of JSON, AVRO, PROTOBUF, or DELIMITED.
  * @param topic The topic backing the stream or table.
  * @param extended A boolean that indicates whether this is an extended description.
  * @param statistics A string that contains statistics about production and consumption to and from the backing topic (extended only).
  * @param errorStats A string that contains statistics about errors producing and consuming to and from the backing topic (extended only).
  * @param replication The replication factor of the backing topic (extended only).
  * @param partitions The number of partitions in the backing topic (extended only).
  *
  * @author Andrey Romanov
  */
case class SourceDescription(
  name: String,
  readQueries: List[String],
  writeQueries: List[String],
  fields: List[FieldInfo],
  `type`: String,
  key: String,
  timestamp: String,
  format: String,
  topic: String,
  extended: Boolean,
  statistics: Option[String],
  errorStats: Option[String],
  replication: Option[Int],
  partitions: Option[Int]
)

object SourceDescription {

  implicit val reader: JsonReader[SourceDescription] = jsonReader[SourceDescription]
  implicit val writer: JsonWriter[SourceDescription] = jsonWriter[SourceDescription]

}

/**
  * Case class for holding the "DESCRIBE" result
  * @param sourceDescription result of "describe" query
  *
  * @author Andrey Romanov
  */
case class DescribeResult(sourceDescription: SourceDescription)

object DescribeResult {

  implicit val reader: JsonReader[DescribeResult] = jsonReader[DescribeResult]
  implicit val writer: JsonWriter[DescribeResult] = jsonWriter[DescribeResult]

}
