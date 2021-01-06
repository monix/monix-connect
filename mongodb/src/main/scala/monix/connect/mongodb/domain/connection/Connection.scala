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
import org.bson.codecs.configuration.{CodecProvider, CodecRegistry}
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY

private[mongodb] trait Connection[A <: Product, B <: Product] {

  def create(url: String, collectionInfo: A): Resource[Task, B] =
    Resource.make(Task.now(MongoClients.create(url)))(_ => Task.unit).flatMap(createUnsafe(_, collectionInfo))

  def create(clientSettings: MongoClientSettings, collections: A): Resource[Task, B] =
    Resource
      .make(Task.now(MongoClients.create(clientSettings)))(_ => Task.unit)
      .flatMap(createUnsafe(_, collections))

  @UnsafeBecauseImpure
  def createUnsafe(client: MongoClient, collections: A): Resource[Task, B]

  def close(connectors: B): Task[Unit]

}

private[mongodb] object Connection {

  private[mongodb] def apply[A]: Connection1[A] = new Connection1[A]
  private[mongodb] def apply[A, B]: Connection2[A, B] = new Connection2[A, B]
  private[mongodb] def apply[A, B, C]: Connection3[A, B, C] = new Connection3[A, B, C]
  private[mongodb] def apply[A, B, C, D]: Connection4[A, B, C, D] = new Connection4[A, B, C, D]
  private[mongodb] def apply[A, B, C, D, E]: Connection5[A, B, C, D, E] = new Connection5[A, B, C, D, E]
  private[mongodb] def apply[A, B, C, D, E, F]: Connection6[A, B, C, D, E, F] = new Connection6[A, B, C, D, E, F]

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
