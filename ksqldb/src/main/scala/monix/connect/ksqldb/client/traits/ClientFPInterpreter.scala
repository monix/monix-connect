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

package monix.connect.ksqldb.client.traits

import sttp.client._

import java.nio.ByteBuffer
import scala.concurrent.duration.Duration

abstract class ClientFPInterpreter[F[_], G[_], WSBackend[_]](baseURL: String) {

  implicit val backend: SttpBackend[F, G[ByteBuffer], WSBackend]

  type SimpleRequest = Request[Either[String, String], Nothing]
  type SimpleResponse = F[Response[Either[String, String]]]
  type StreamingRequest = RequestT[Identity, Either[String, G[ByteBuffer]], G[ByteBuffer]]
  type StreamingResponse = F[Response[Either[String, G[ByteBuffer]]]]

  private val ksqlSimpleHeader: String = "application/vnd.ksql.v1+json"
  private val ksqlPullQueryHeader: String = "application/vnd.ksqlapi.delimited.v1"

  def sendGETRequest(requestURL: String): SimpleResponse = {

    val request: SimpleRequest = basicRequest
      .header("Accept", ksqlSimpleHeader)
      .header("Content-Type", ksqlSimpleHeader)
      .get(uri"$requestURL")

    request.send()

  }

  def sendPOSTRequest(requestURL: String, inputBody: Option[String]): SimpleResponse = {

    val request: SimpleRequest = basicRequest
      .header("Accept", ksqlSimpleHeader)
      .header("Content-Type", ksqlSimpleHeader)
      .post(uri"$requestURL")

    inputBody match {
      case Some(value) => request.body(value).send()
      case None => request.send()
    }

  }

  def sendStreamRequest(
    requestURL: String,
    inputBody: String,
    isPullQuery: Boolean = false
  ): StreamingResponse = {

    val ksqlHeader: String = isPullQuery match {
      case false => ksqlSimpleHeader
      case true => ksqlPullQueryHeader
    }

    val request: StreamingRequest = basicRequest
      .header("Accept", ksqlHeader)
      .header("Content-Type", ksqlHeader)
      .post(uri"$requestURL")
      .body(inputBody)
      .response(asStream[G[ByteBuffer]])
      .readTimeout(Duration.Inf)

    request.send()

  }

}
