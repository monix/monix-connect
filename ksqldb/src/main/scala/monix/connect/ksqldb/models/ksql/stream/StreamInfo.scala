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

package monix.connect.ksqldb.models.ksql.stream

import tethys._
import tethys.derivation.semiauto._

/**
  * Class with information about retrieved streams
  *
  * @param name stream name
  * @param topic topic, which is consumed by stream
  * @param format serialization format for data in stream (JSON, AVRO, DELIMITED)
  * @param type type information
  *
  * @author Andrey Romanov
  */
case class StreamInfo(name: String, topic: String, format: String, `type`: String)

object StreamInfo {

  implicit val reader: JsonReader[StreamInfo] = jsonReader[StreamInfo]
  implicit val writer: JsonWriter[StreamInfo] = jsonWriter[StreamInfo]

}
