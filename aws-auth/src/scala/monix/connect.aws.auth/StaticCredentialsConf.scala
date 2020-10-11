package monix.connect.aws.auth

import monix.execution.internal.InternalApi

@InternalApi
private[connect] case class StaticCredentialsConf(accessKeyId: String, secretAccessKey: String, sessionToken: Option[String])
