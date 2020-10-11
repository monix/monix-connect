package monix.connect.aws.auth

case class StaticCredentialsConf(accessKeyId: String, secretAccessKey: String, sessionToken: Option[String])
