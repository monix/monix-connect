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
import com.mongodb.MongoClientSettings
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients, MongoDatabase}
import monix.connect.mongodb.domain.{Collection, MongoConnector}
import monix.connect.mongodb.{MongoDb, MongoSingle, MongoSink, MongoSource}
import monix.eval.Task
import monix.execution.annotations.UnsafeBecauseImpure
import monix.reactive.Observable
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY

private[mongodb] trait Connection[A <: Product, T2 <: Product] {

  def create(url: String, collectionInfo: A): Resource[Task, T2] =
    Resource.make(Task.now(MongoClients.create(url)))(_ => Task.unit).flatMap(createUnsafe(_, collectionInfo))

  def create(clientSettings: MongoClientSettings, collections: A): Resource[Task, T2] =
    Resource
      .make(Task.now(MongoClients.create(clientSettings)))(_ => Task.unit)
      .flatMap(createUnsafe(_, collections))

  @UnsafeBecauseImpure
  def createUnsafe(client: MongoClient, collections: A): Resource[Task, T2]

  def close(connectors: T2): Task[Unit] = {
    Observable.fromIterable(connectors.productIterator.toList)
      .map(_.asInstanceOf[MongoConnector].close)
      .completedL
  }

}

private[mongodb] object Connection {

  private[mongodb] def apply[T1]: Connection1[T1] = new Connection1[T1]
  private[mongodb] def apply[T1, T2]: Connection2[T1, T2] = new Connection2[T1, T2]
  private[mongodb] def apply[T1, T2, T3]: Connection3[T1, T2, T3] = new Connection3[T1, T2, T3]
  private[mongodb] def apply[T1, T2, T3, T4]: Connection4[T1, T2, T3, T4] = new Connection4[T1, T2, T3, T4]
  private[mongodb] def apply[T1, T2, T3, T4, T5]: Connection5[T1, T2, T3, T4, T5] = new Connection5[T1, T2, T3, T4, T5]
  private[mongodb] def apply[T1, T2, T3, T4, T5, T6]: Connection6[T1, T2, T3, T4, T5, T6] = new Connection6[T1, T2, T3, T4, T5, T6]
  private[mongodb] def apply[T1, T2, T3, T4, T5, T6, T7]: Connection7[T1, T2, T3, T4, T5, T6, T7] = new Connection7[T1, T2, T3, T4, T5, T6, T7]
  private[mongodb] def apply[T1, T2, T3, T4, T5, T6, T7, T8]: Connection8[T1, T2, T3, T4, T5, T6, T7, T8] = new Connection8[T1, T2, T3, T4, T5, T6, T7, T8]

  private[mongodb] def fromCodecProvider(codecRegistry: CodecProvider*): CodecRegistry =
    fromRegistries(fromProviders(codecRegistry: _*), DEFAULT_CODEC_REGISTRY)

  private[mongodb] def createConnector[A](
    client: MongoClient,
    collectionInfo: Collection[A],
    codecRegistry: CodecRegistry): Task[MongoConnector[A]] = {
    val db: MongoDatabase = client.getDatabase(collectionInfo.databaseName).withCodecRegistry(codecRegistry)
    MongoDb.createIfNotExists(db, collectionInfo.collectionName).map { _ =>
      val col = db.getCollection(collectionInfo.collectionName, collectionInfo.clazz)
      MongoConnector(MongoDb(client, db), MongoSource(col), MongoSingle(col), MongoSink(col))
    }
  }
}
