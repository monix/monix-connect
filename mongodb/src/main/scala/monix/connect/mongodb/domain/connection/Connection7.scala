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
import monix.connect.mongodb.domain.{Collection, MongoConnector, Tuple6F, Tuple7F}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure

private[mongodb] class Connection7[T1, T2, T3, T4, T5, T6, T7]
  extends Connection[
    Tuple7F[Collection, T1, T2, T3, T4, T5, T6, T7],
    Tuple7F[MongoConnector, T1, T2, T3, T4, T5, T6, T7]] { self =>

  @UnsafeBecauseImpure
  override def createUnsafe(client: MongoClient, collections: Tuple7F[Collection, T1, T2, T3, T4, T5, T6, T7])
    : Resource[Task, Tuple7F[MongoConnector, T1, T2, T3, T4, T5, T6, T7]] = {
    Resource.make(Connection7.createConnectors(client, collections))(self.close)
  }

}

private[mongodb] object Connection7 {

  def createConnectors[T1, T2, T3, T4, T5, T6, T7](
    client: MongoClient,
    collections: Tuple7F[Collection, T1, T2, T3, T4, T5, T6, T7])
    : Task[Tuple7F[MongoConnector, T1, T2, T3, T4, T5, T6, T7]] = {
    val (a, b, c, d, e, f, g) = collections
    for {
      a <- createConnector(client, a, fromCodecProvider(a.codecProvider: _*))
      t <- Connection6.createConnectors(client, (b, c, d, e, f, g))
    } yield (a, t._1, t._2, t._3, t._4, t._5, t._6)
  }
}
