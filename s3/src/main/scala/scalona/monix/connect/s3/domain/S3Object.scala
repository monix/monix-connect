package scalona.monix.connect.s3.domain

case class S3Object(bucket: String, key: String, content: String)
