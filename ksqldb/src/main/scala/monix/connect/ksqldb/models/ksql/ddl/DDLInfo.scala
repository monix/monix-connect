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

package monix.connect.ksqldb.models.ksql.ddl

import tethys._

/**
  * Class for representing execution result for such queries, like `CREATE`, `DROP`, `TERMINATE`
  *
  * @param statement ksql statement
  * @param commandID command, which was created by statement (ID for status retrieve)
  * @param status result of statement
  * @param commandSequenceNumber sequence number of operation
  *
  * @author Andrey Romanov
  */
case class DDLInfo(
  statement: String,
  commandID: String,
  status: CommandStatus,
  commandSequenceNumber: Long
)

object DDLInfo {

  implicit val reader: JsonReader[DDLInfo] = JsonReader.builder
    .addField[String]("statementText")
    .addField[String]("commandId")
    .addField[CommandStatus]("commandStatus")
    .addField[Long]("commandSequenceNumber")
    .buildReader(DDLInfo.apply)

  implicit val writer: JsonWriter[DDLInfo] = JsonWriter
    .obj[DDLInfo]
    .addField("statementText")(_.statement)
    .addField("commandId")(_.commandID)
    .addField("commandStatus")(_.status)
    .addField("commandSequenceNumber")(_.commandSequenceNumber)

}
