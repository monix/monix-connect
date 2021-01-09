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
  extends Connection[Tuple3F[Collection, A, B, C], Tuple3F[MongoConnector, A, B, C]] { self =>

  @UnsafeBecauseImpure
  override def createUnsafe(
    client: MongoClient,
    collections: Tuple3F[Collection, A, B, C]): Resource[Task, Tuple3F[MongoConnector, A, B, C]] = {
    Resource.make(Connection3.createConnectors(client, collections))(self.close)
  }

}

private[mongodb] object Connection3 {


  def createConnectors[A, B, C](
                                 client: MongoClient,
                                 collections: Tuple3F[Collection, A, B, C]): Task[Tuple3F[MongoConnector, A, B, C]] = {
    for {
      a <- Connection.createConnector(client, collections._1, fromCodecProvider(collections._1.codecProvider: _*))
      t <- Connection2.createConnectors(client, collections._2, collections._3)
    } yield (a, t._1, t._2)
  }

}
