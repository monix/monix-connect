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
  * Information about KSQL server, which was retrieved by REST Call
  *
  * @param version server version
  * @param clusterID KSQL cluster ID
  * @param serviceID KSQL service ID
  *
  * @author Andrey Romanov
  */
case class VersionInfo(version: String, clusterID: String, serviceID: String)

object VersionInfo {

  implicit val reader: JsonReader[VersionInfo] = JsonReader.builder
    .addField[String]("version")
    .addField[String]("kafkaClusterId")
    .addField[String]("ksqlServiceId")
    .buildReader(VersionInfo.apply)

  implicit val writer: JsonWriter[VersionInfo] = JsonWriter
    .obj[VersionInfo]
    .addField("version")(_.version)
    .addField("kafkaClusterId")(_.clusterID)
    .addField("ksqlServiceId")(_.serviceID)

}

/**
  * Class for incapsulating most server information
  *
  * @param info information about KSQL server
  *
  * @author Andrey Romanov
  * @since 0.0.1
  */
case class KSQLVersionResponse(info: VersionInfo)

object KSQLVersionResponse {

  implicit val reader: JsonReader[KSQLVersionResponse] = JsonReader.builder
    .addField[VersionInfo]("KsqlServerInfo")
    .buildReader(KSQLVersionResponse.apply)

  implicit val writer: JsonWriter[KSQLVersionResponse] = JsonWriter
    .obj[KSQLVersionResponse]
    .addField("KsqlServerInfo")(_.info)

}
