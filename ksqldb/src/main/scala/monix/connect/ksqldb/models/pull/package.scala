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

import org.json4s.JsonAST.JArray
import tethys._
import tethys.jackson._
import tethys.readers.ReaderError
import org.json4s._
import org.json4s.jackson.JsonMethods._

package object pull {

  val errorMessage = "Could not deserialize pull response body"

  def processData(body: String): Either[ClientError, PullResponse] = {

    parseOpt(body) match {
      case None => Left(ClientError(errorMessage, None, None))
      case Some(value) =>
        Right(PullResponse(isSchema = false, None, Some(value.asInstanceOf[JArray])))
    }

  }

  def convert(body: String): Either[ClientError, PullResponse] = {

    val schemaAttempt: Either[ReaderError, ResponseSchema] = body.jsonAs[ResponseSchema]

    schemaAttempt match {
      case Left(_) => processData(body)
      case Right(value) => Right(PullResponse(isSchema = true, Some(value), None))
    }

  }

}
