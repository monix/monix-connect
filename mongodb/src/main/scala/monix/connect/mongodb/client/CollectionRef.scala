package monix.connect.mongodb.client

import org.bson.codecs.configuration.CodecProvider

final case class CollectionRef[Doc](
                               databaseName: String,
                               collectionName: String,
                               clazz: Class[Doc],
                               codecProvider: CodecProvider*)
