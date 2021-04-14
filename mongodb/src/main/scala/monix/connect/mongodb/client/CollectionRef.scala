package monix.connect.mongodb.client

import org.bson.codecs.configuration.CodecProvider
import org.bson.conversions.Bson

sealed trait CollectionRef[Doc] {
 val databaseName: String
 val collectionName: String
}

final case class CollectionCodec[Doc](databaseName: String,
                                        collectionName: String,
                                        clazz: Class[Doc],
                                             codecProviders: CodecProvider*) extends CollectionRef[Doc]

final case class CollectionBson(databaseName: String,
                                     collectionName: String) extends CollectionRef[Bson]
