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
import monix.connect.mongodb.domain.{Collection, MongoConnector}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure

private[mongodb] class Connection1[A] extends Connection[Collection[A], MongoConnector[A]] { self =>
  @UnsafeBecauseImpure
  override def createUnsafe(client: MongoClient, collectionInfo: Collection[A]): Resource[Task, MongoConnector[A]] = {
    val codecRegistry = fromCodecProvider(collectionInfo.codecProvider: _*)
    Resource.make { Connection.createConnector(client, collectionInfo, codecRegistry) }(self.close)
  }

  override def close(connectors: MongoConnector[A]): Task[Unit] = connectors.close
}
