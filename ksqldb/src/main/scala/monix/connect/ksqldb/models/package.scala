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

package monix.connect.ksqldb

import tethys._
import tethys.jackson._
import tethys.JsonReader
import tethys.readers.ReaderError

package object models {

  val errorMessage = "Error while processing response body"

  def processLeft[A: JsonReader](body: String): Left[ClientError, Nothing] = {

    val errorBody: Either[ReaderError, ExecutionError] = body.jsonAs[ExecutionError]

    errorBody match {
      case Left(value) => Left(ClientError(errorMessage, None, Some(value.getMessage)))
      case Right(value) => Left(ClientError(errorMessage, Some(value), None))
    }

  }

  def processRight[A: JsonReader](body: String): Either[ClientError, A] = {

    val queryStatus: Either[ReaderError, A] = body.jsonAs[A]

    queryStatus match {
      case Left(value) => Left(ClientError(errorMessage, None, Some(value.getMessage)))
      case Right(value) => Right(value)
    }

  }

  def processBody[A: JsonReader](body: Either[String, String]): Either[ClientError, A] = {

    body match {
      case Left(value) => processLeft(value)
      case Right(value) => processRight(value)
    }

  }

}
