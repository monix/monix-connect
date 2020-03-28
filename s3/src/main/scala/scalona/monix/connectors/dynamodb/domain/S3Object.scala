package scalarc.monix.connectors.dynamodb.domain

case class S3Object(bucket: String, key: String, content: String)
