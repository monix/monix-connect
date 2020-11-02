package monix.connect

import java.net.URI

import monix.reactive.Observable
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

package object dynamodb {

  type Transformer[A, B] = Observable[A] => Observable[B]

  implicit class ObservableExtension[A](ob: Observable[A]) {
    def transform[B](transformer: Transformer[A, B]): Observable[B] = {
      transformer(ob)
    }
  }

  object DynamoDbClient {
    def apply(): DynamoDbAsyncClient = {

      val defaultAwsCredProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
      DynamoDbAsyncClient
        .builder()
        .credentialsProvider(defaultAwsCredProvider)
        .endpointOverride(new URI("http://localhost:4569"))
        .region(Region.AWS_GLOBAL)
        .build()
    }
  }

}
