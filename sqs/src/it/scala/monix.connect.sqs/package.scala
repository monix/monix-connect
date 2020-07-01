package monix.connect

import java.net.URI

import monix.reactive.Observable
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient

package object sqs {

  type Transformer[A, B] = Observable[A] => Observable[B]

  implicit class ObservableExtension[A](ob: Observable[A]) {
    def transform[B](transformer: Transformer[A, B]): Observable[B] = {
      transformer(ob)
    }
  }

  object SqsClient {
    def apply(): SqsAsyncClient = {

      val defaultAwsCredProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
      SqsAsyncClient
        .builder()
        .credentialsProvider(defaultAwsCredProvider)
        .endpointOverride(new URI("http://localhost:4576"))
        .region(Region.US_EAST_1)
        .build()
    }
  }

}
