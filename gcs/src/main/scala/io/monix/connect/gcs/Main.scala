package io.monix.connect.gcs

import io.monix.connect.gcs.configuration.BucketConfig
import monix.eval.Task

object Main {
  val config: BucketConfig = BucketConfig("mybucket")
  val bucket: Task[Bucket] = Bucket(config)
}
