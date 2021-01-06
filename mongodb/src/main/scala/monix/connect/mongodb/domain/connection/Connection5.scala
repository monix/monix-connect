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
import monix.connect.mongodb.domain.connection.Connection.fromCodecProvider
import monix.connect.mongodb.domain.{Collection, MongoConnector, Tuple4F, Tuple5F}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure

private[mongodb] class Connection5[T1, T2, T3, T4, T5]
  extends Connection[Tuple5F[Collection, T1, T2, T3, T4, T5], Tuple5F[MongoConnector, T1, T2, T3, T4, T5]] {

  @UnsafeBecauseImpure
  override def createUnsafe(client: MongoClient, collections: Tuple5F[Collection, T1, T2, T3, T4, T5])
    : Resource[Task, Tuple5F[MongoConnector, T1, T2, T3, T4, T5]] = {
    val (a, b, c, d, e) = collections
    Resource.make(Connection5.createConnectors(client, a, (b, c, d, e)))(connector =>
      Task(connector._1.db.client.close()))
  }

  override def close(
    connectors: (MongoConnector[T1], MongoConnector[T2], MongoConnector[T3], MongoConnector[T4], MongoConnector[T5]))
    : Task[Unit] =
    Task.map5(connectors._1.close, connectors._2.close, connectors._3.close, connectors._4.close, connectors._5.close)(
      (_, _, _, _, _) => ())

}

private[mongodb] object Connection5 {
  def createConnectors[A, B, C, D, E](
    client: MongoClient,
    current: Collection[A],
    next: Tuple4F[Collection, B, C, D, E]): Task[Tuple5F[MongoConnector, A, B, C, D, E]] = {
    for {
      a <- Connection.createConnector(client, current, fromCodecProvider(current.codecProvider: _*))
      t <- Connection4.createConnectors(client, next._1, (next._2, next._3, next._4))
    } yield (a, t._1, t._2, t._3, t._4)
  }

}
