package monix.connect.mongodb.client

import org.bson.codecs.configuration.CodecProvider

sealed trait CollectionRef {
 val databaseName: String
 val collectionName: String
}

final case class CollectionCodec[Doc](databaseName: String,
                                        collectionName: String,
                                        clazz: Class[Doc],
                                             codecProviders: List[CodecProvider]) extends CollectionRef

final case class CollectionBson(databaseName: String,
                                     collectionName: String) extends CollectionRef
