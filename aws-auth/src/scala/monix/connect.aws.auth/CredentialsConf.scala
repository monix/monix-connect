package monix.connect.aws.auth

import software.amazon.awssdk.auth.credentials.{AnonymousCredentialsProvider, AwsBasicCredentials, AwsCredentialsProvider, AwsSessionCredentials, DefaultCredentialsProvider, StaticCredentialsProvider}

case class CredentialsConf(provider: Provider.Type, staticCredentials: Option[StaticCredentialsConf]) {
  val credentialsProvider: AwsCredentialsProvider = {
    provider match {
      case Provider.Anonymous => AnonymousCredentialsProvider.create()
      case Provider.Static =>
        staticCredentials match {
          case Some(creeds) =>
            StaticCredentialsProvider.create {
              creeds.sessionToken match {
                case None => AwsBasicCredentials.create(creeds.accessKeyId, creeds.secretAccessKey)
                case Some(token) => AwsSessionCredentials.create(creeds.accessKeyId, creeds.secretAccessKey, token)
              }
            }
          case None => {
            println("WARN! - Basic credentials is empty!")
            DefaultCredentialsProvider.create()
          }
        }
      case Provider.Default => DefaultCredentialsProvider.create()
      case _ => DefaultCredentialsProvider.create()
    }
  }
}