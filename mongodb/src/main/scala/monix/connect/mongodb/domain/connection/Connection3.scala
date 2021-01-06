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
import monix.connect.mongodb.domain.{Collection, MongoConnector, Tuple2F, Tuple3F}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure

private[mongodb] class Connection3[A, B, C]
  extends Connection[Tuple3F[Collection, A, B, C], Tuple3F[MongoConnector, A, B, C]] {

  @UnsafeBecauseImpure
  override def createUnsafe(
    client: MongoClient,
    collections: Tuple3F[Collection, A, B, C]): Resource[Task, Tuple3F[MongoConnector, A, B, C]] = {
    val (a, b, c) = collections
    Resource.make(Connection3.createConnectors(client, a, (b, c)))(connector => Task(connector._1.db.client.close()))
  }

  override def close(connectors: (MongoConnector[A], MongoConnector[B], MongoConnector[C])): Task[Unit] =
    Task.map3(connectors._1.close, connectors._2.close, connectors._3.close)((_, _, _) => ())
}

private[mongodb] object Connection3 {

  def createConnectors[A, B, C](
    client: MongoClient,
    current: Collection[A],
    next: Tuple2F[Collection, B, C]): Task[Tuple3F[MongoConnector, A, B, C]] = {
    for {
      a <- Connection.createConnector(client, current, fromCodecProvider(current.codecProvider: _*))
      t <- Connection2.createConnectors(client, next._1, next._2)
    } yield (a, t._1, t._2)
  }

}
