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

package monix.connect.mongodb.domain.connection

import cats.effect.Resource
import com.mongodb.reactivestreams.client.MongoClient
import monix.connect.mongodb.domain.connection.Connection.{createConnector, fromCodecProvider}
import monix.connect.mongodb.domain.{Collection, MongoConnector, Tuple5F, Tuple6F}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure

private[mongodb] class Connection6[T1, T2, T3, T4, T5, T6]
  extends Connection[Tuple6F[Collection, T1, T2, T3, T4, T5, T6], Tuple6F[MongoConnector, T1, T2, T3, T4, T5, T6]] {

  @UnsafeBecauseImpure
  override def createUnsafe(client: MongoClient, collections: Tuple6F[Collection, T1, T2, T3, T4, T5, T6])
    : Resource[Task, Tuple6F[MongoConnector, T1, T2, T3, T4, T5, T6]] = {
    val (a, b, c, d, e, f) = collections
    Resource.make(Connection6.createConnectors(client, a, (b, c, d, e, f)))(connector =>
      Task(connector._1.db.client.close()))
  }

  override def close(
    connectors: (
      MongoConnector[T1],
      MongoConnector[T2],
      MongoConnector[T3],
      MongoConnector[T4],
      MongoConnector[T5],
      MongoConnector[T6])): Task[Unit] =
    Task.map6(
      connectors._1.close,
      connectors._2.close,
      connectors._3.close,
      connectors._4.close,
      connectors._5.close,
      connectors._5.close)((_, _, _, _, _, _) => ())
}

private[mongodb] object Connection6 {

  def createConnectors[T1, T2, T3, T4, T5, T6](
    client: MongoClient,
    current: Collection[T1],
    next: Tuple5F[Collection, T2, T3, T4, T5, T6]): Task[Tuple6F[MongoConnector, T1, T2, T3, T4, T5, T6]] = {
    for {
      a <- createConnector(client, current, fromCodecProvider(current.codecProvider: _*))
      t <- Connection5.createConnectors(client, next._1, (next._2, next._3, next._4, next._5))
    } yield (a, t._1, t._2, t._3, t._4, t._5)
  }
}
