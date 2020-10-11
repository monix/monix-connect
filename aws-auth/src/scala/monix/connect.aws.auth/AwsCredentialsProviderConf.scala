package monix.connect.aws.auth

import monix.execution.internal.InternalApi
import software.amazon.awssdk.auth.credentials.{AnonymousCredentialsProvider, AwsBasicCredentials, AwsCredentialsProvider, AwsSessionCredentials, DefaultCredentialsProvider, EnvironmentVariableCredentialsProvider, InstanceProfileCredentialsProvider, ProfileCredentialsProvider, StaticCredentialsProvider, SystemPropertyCredentialsProvider}

@InternalApi
private[connect] case class AwsCredentialsProviderConf(provider: Provider.Type, profileName: Option[String], staticCredentials: Option[StaticCredentialsConf]) {
  val credentialsProvider: AwsCredentialsProvider = {
    provider match {
      case Provider.Anonymous => AnonymousCredentialsProvider.create()
      case Provider.Default => DefaultCredentialsProvider.create()
      case Provider.Environment => EnvironmentVariableCredentialsProvider.create()
      case Provider.Instance => InstanceProfileCredentialsProvider.create()
      case Provider.Profile => {
        profileName match {
          case Some(name) => ProfileCredentialsProvider.create(name)
          case None => ProfileCredentialsProvider.create()
        }
      }
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
      case Provider.System => SystemPropertyCredentialsProvider.create()
      case _ => DefaultCredentialsProvider.create()
    }
  }
}