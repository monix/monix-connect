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
import monix.connect.mongodb.domain.{Collection, MongoConnector, Tuple3F, Tuple4F}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure

private[mongodb] class Connection4[T1, T2, T3, T4]
  extends Connection[Tuple4F[Collection, T1, T2, T3, T4], Tuple4F[MongoConnector, T1, T2, T3, T4]] { self =>

  @UnsafeBecauseImpure
  override def createUnsafe(
    client: MongoClient,
    collections: Tuple4F[Collection, T1, T2, T3, T4]): Resource[Task, Tuple4F[MongoConnector, T1, T2, T3, T4]] = {
    Resource.make(Connection4.createConnectors(client, collections))(self.close)
  }

}

object Connection4 {
  def createConnectors[A, B, C, D](
    client: MongoClient,
    collections: Tuple4F[Collection, A, B, C, D]): Task[Tuple4F[MongoConnector, A, B, C, D]] = {
    for {
      a <- Connection.createConnector(client, collections._1, fromCodecProvider(collections._1.codecProvider: _*))
      t <- Connection3.createConnectors(client, (collections._2, collections._3, collections._4))
    } yield (a, t._1, t._2, t._3)
  }

}
