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

package monix.connect.mongodb.client

import monix.connect.mongodb.{MongoDb, MongoSingle, MongoSink, MongoSource}
import monix.eval.Task

/**
  * Comprehends the set of classes needed to communicate and operate with mongodb.
  *
  * @param db provides operations to create, drop, list and check
  *           existence of both databases and collections.
  * @param source provides operations that requires reading from database,
  *               like finding or counting documents.
  * @param single provides operations to execute a single task, like inserting,
  *               replacing or deleting documents at once.
  * @param sink provides the same operations as [[MongoSingle]], but it is
  *             designed and exposed as a reactive subscriber [[Consumer]].
  */
final case class CollectionOperator[Doc](
  db: MongoDb,
  source: MongoSource[Doc],
  single: MongoSingle[Doc],
  sink: MongoSink[Doc])
