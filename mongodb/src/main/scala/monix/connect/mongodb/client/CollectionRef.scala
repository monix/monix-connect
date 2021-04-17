package monix.connect.mongodb.client

import org.bson.Document
import org.bson.codecs.configuration.CodecProvider

trait CollectionRef[+Doc] {
 val database: String
 val collection: String
}

/**
  * Represents the reference to a collection with a custom codec.
  *
  * ==Example==
  * {{{
  *   case class Employee(name: String, age: Int, city: String, companyName: String, activities: List[String])
  * }}}
  */
final case class CollectionCodecRef[Doc](database: String,
                                         collection: String,
                                         clazz: Class[Doc],
                                         codecProviders: CodecProvider*) extends CollectionRef[Doc]

final case class CollectionDocumentRef(database: String,
                                       collection: String) extends CollectionRef[Document]
