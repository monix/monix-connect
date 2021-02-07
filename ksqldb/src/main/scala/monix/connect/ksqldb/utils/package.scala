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

import java.nio.ByteBuffer

import monix.connect.ksqldb.models.pull.{convert, PullResponse}
import monix.connect.ksqldb.models.push.PushResponse
import monix.connect.ksqldb.models.{ClientError, ExecutionError}
import monix.connect.ksqldb.models.query.row.RowInfo
import monix.execution.atomic.AtomicAny
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import monix.nio.text.UTF8Codec._
import tethys._
import tethys.jackson._
import tethys.readers.ReaderError

/**
  * Utils package with some common things
  *
  * @author Andrey Romanov
  */
package object utils {
  type AtomicSubscriber = AtomicAny[Option[Subscriber[Array[Byte]]]]
  val chunkSize: Int = 200
  val lineTerm: Array[Byte] = Array('\n'.toByte)

  def processLeft(body: String): Left[ClientError, Nothing] = {

    val errorMessage = "Error while working with stream of response body"

    val errorBody: Either[ReaderError, ExecutionError] = body.jsonAs[ExecutionError]

    if (errorBody.isLeft) {
      Left(ClientError(errorMessage, None, Some(errorBody.swap.toOption.get.getMessage)))
    }

    Left(ClientError(errorMessage, Some(errorBody.toOption.get), None))

  }

  def prepareObservable(body: Observable[ByteBuffer]): Observable[String] = {

    body.map { elem =>
      val byteArr: Array[Byte] = new Array[Byte](elem.remaining())
      elem.get(byteArr)
      elem.rewind()

      byteArr

    }.pipeThrough(Framing(lineTerm, chunkSize))
      .pipeThrough(utf8Decode)

  }

  def processRowInfo(
    body: Either[String, Observable[ByteBuffer]]
  ): Either[ClientError, Observable[Either[ClientError, RowInfo]]] = {

    val errorMessage = "Error while processing response body for row information"

    body match {
      case Left(value) => processLeft(value)
      case Right(value) => {

        val readResult: Observable[Either[ClientError, RowInfo]] =
          prepareObservable(value)
            .map(elem => elem.jsonAs[RowInfo])
            .map { elem =>
              if (elem.isLeft) {
                Left(ClientError(errorMessage, None, Some(elem.swap.toOption.get.getMessage)))
              } else {
                Right(elem.toOption.get)
              }

            }

        Right(readResult)

      }
    }

  }

  def processPullQuery(
    body: Either[String, Observable[ByteBuffer]]
  ): Either[ClientError, Observable[Either[ClientError, PullResponse]]] = {

    body match {

      case Left(value) => processLeft(value)
      case Right(value) => {

        val readResult =
          prepareObservable(value)
            .map(elem => convert(elem))

        Right(readResult)

      }

    }

  }

  def processPushQuery(
    body: Either[String, Observable[ByteBuffer]]
  ): Either[ClientError, Observable[Either[ClientError, PushResponse]]] = {

    val errorMessage = "Error while processing response body for push query result"

    body match {

      case Left(value) => processLeft(value)
      case Right(value) => {

        val readResult =
          prepareObservable(value)
            .map(elem => elem.jsonAs[PushResponse])
            .map { elem =>
              if (elem.isLeft) {
                Left(ClientError(errorMessage, None, Some(elem.swap.toOption.get.getMessage)))
              } else {
                Right(elem.toOption.get)
              }

            }

        Right(readResult)

      }

    }

  }

}
