package monix.connect.mongodb.client

import monix.connect.mongodb.{MongoDb, MongoSingle, MongoSink, MongoSource}
import monix.eval.Task

final case class CollectionOperator[Doc](db: MongoDb, source: MongoSource[Doc], single: MongoSingle[Doc], sink: MongoSink[Doc])
