package monix.connect.dynamodb

import java.net.URI

import monix.connect.dynamodb.DynamoAppConfig.DynamoDbConfig
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object DynamoDbClient {
  def apply(): DynamoDbAsyncClient = {
    val config: DynamoDbConfig = DynamoAppConfig.load()
    DynamoDbAsyncClient
      .builder()
      .credentialsProvider(config.awsCredProvider)
      .endpointOverride(new URI(config.endPoint))
      .region(Region.AWS_GLOBAL)
      .build()
  }
}
