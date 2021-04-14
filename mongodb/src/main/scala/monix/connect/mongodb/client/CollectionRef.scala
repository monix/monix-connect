package monix.connect.mongodb.client

import org.bson.codecs.configuration.CodecProvider

final case class CollectionRef[Doc](
                                     databaseName: String,
                                     collectionName: String) {

  private[mongodb] var clazz: Option[Class[Doc]]

  private[mongodb] var codecProvider: List[CodecProvider] = List.empty

  def withClazz(clazz: Class[Doc])
  def withCodec(codecProviders: CodecProvider*): CollectionRef[Doc] = {
    codecProvider = codecProviders.toList
    this
  }
}
