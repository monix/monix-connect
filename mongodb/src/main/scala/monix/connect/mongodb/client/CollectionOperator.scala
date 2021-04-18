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
final case class CollectionOperator[Doc](db: MongoDb, source: MongoSource[Doc], single: MongoSingle[Doc], sink: MongoSink[Doc])
