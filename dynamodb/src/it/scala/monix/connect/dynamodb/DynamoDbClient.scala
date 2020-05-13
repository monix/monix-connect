package monix.connect.dynamodb

import java.net.URI

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

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
