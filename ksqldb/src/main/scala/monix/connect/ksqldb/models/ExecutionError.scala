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

package monix.connect.ksqldb.models

import tethys._

/**
  * Class for representing an error instance from KSQL Server
  *
  * @param errorCode error code from KSQL server
  * @param message error message from KSQL server
  *
  * @author Andrey Romanov
  */
case class ExecutionError(errorCode: String, message: String)

object ExecutionError {

  implicit val reader: JsonReader[ExecutionError] = JsonReader.builder
    .addField[String]("error_code")
    .addField[String]("message")
    .buildReader(ExecutionError.apply)

  implicit val writer: JsonWriter[ExecutionError] = JsonWriter
    .obj[ExecutionError]
    .addField("error_code")(_.errorCode)
    .addField("message")(_.message)

}

/**
  * Case class for client error (by many backends)
  *
  * @param message error message
  * @param description error description
  * @param traceback description in case of deserialization error
  * @author Andrey Romanov
  * @since 0.0.1
  */
case class ClientError(
  message: String,
  description: Option[ExecutionError],
  traceback: Option[String]
)
